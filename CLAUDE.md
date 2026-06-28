# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`inertia-relayer` is a Solana TPU relayer and Jito Block Engine proxy (Rust, edition 2024, `agave`/`solana` 4.0). It sits between the public network and a single validator:

- Receives transactions over QUIC on the TPU ports, sig-verifies them, and forwards verified packet batches to the validator over gRPC.
- Proxies the validator's connection to the Jito Block Engine (packets + bundles + auth), injecting locally-sourced packets/bundles into those streams.
- Mirrors all observed traffic to a private **Inertia server** over a pinned-cert QUIC channel, and ingests bundles back from it.

It is modeled on Jito's `jito-relayer`; the protobufs and auth scheme are Jito-compatible.

## Build & run

```bash
cargo build                  # debug; logs to stderr at debug level
cargo build --release        # release profile: lto="fat", codegen-units=1, opt-level=3
cargo check                  # fast type-check
cargo clippy
```

- **`protoc` is NOT required on the system.** `build.rs` pulls it from the `protobuf-src` build dependency and sets `PROTOC` automatically. The protos in `src/protos/protos/*.proto` are compiled at build time by `tonic-prost-build`.
- **`.cargo/config.toml` sets `--cfg tokio_unstable`** — required for `Builder::disable_lifo_slot()` in `main.rs`. Don't remove it.
- There are currently **no tests** in the repo (`cargo test` compiles and runs nothing meaningful).

Running requires keypair + JWT PEM keypair + upstream addresses. All CLI args are also env-backed (clap `env` feature), so any `--foo-bar` flag can be set via `FOO_BAR`:

```bash
cargo run --release -- \
  --keypair-path <validator-identity.json> \
  --signing-key-pem-path <jwt-private.pem> \
  --verifying-key-pem-path <jwt-public.pem> \
  --blockengine-url https://<jito-region>.mainnet.block-engine.jito.wtf \
  --inertia-server <ip:port> \
  --inertia-cert-sha256 <64-hex-sha256-of-inertia-server-cert>
```

Default ports (all overridable): relayer gRPC `11225`, block engine gRPC `11226`, proxy/mirror QUIC client bind `11227`, TPU QUIC `11228`, TPU QUIC forward `11229`. Release builds log to `--log-path` (default `/etc/inertia-relayer/relayer.log`) at info level.

## Architecture

Wiring lives in `main.rs`, which builds components in dependency order: `Rpc → Relayer → Blockengine → Proxy`. Each component's `new()` returns its handle plus the background thread/task `JoinHandle`s that `main` awaits at the end.

### Concurrency model

The process deliberately mixes **OS threads** and a **single multi-threaded Tokio runtime**:

- Hot / blocking / CPU paths run on named `std::thread`s: TPU sigverify (`SigVerifyStage`), the forwarder event loop, the staked-nodes updater, the slot subscriber, and the proxy's relayer-delay queue.
- gRPC servers (tonic) and the QUIC mirror run as Tokio tasks on `rt`.
- Cross-boundary handoff uses `crossbeam_channel` (sync side) and `tokio::sync::broadcast`/`watch`/`mpsc` (async side).

Shutdown is a single `Arc<AtomicBool>` `exit` created by `helper::graceful_panic`. The panic hook sets `exit` (fail-fast: any panic brings the whole process down after a 5s grace period). Threads poll `exit`; async tasks use `helper::wait_for_exit` / `shutdown_signal` (SIGTERM/Ctrl-C). When adding a long-running loop, thread it the same `exit` and check it.

### Data flow (the big picture)

```
                    QUIC TPU :11228/:11229
                            │
                  Tpu (stake-weighted QoS) → SigVerifyStage
                            │  BankingPacketBatch (crossbeam)
                            ▼
   ┌──────────────── Proxy (50ms PACKET_DELAY) ───────────────┐
   │   mirror copy ──► Inertia server (QUIC datagrams)         │
   │   delayed batch ─► Forwarder gRPC ──► validator subscribe │
   └──────────────────────────────────────────────────────────┘

   Validator ──► local Blockengine gRPC :11226 ──► Jito Block Engine (upstream)
                            ▲                              │
                            └── injects packets/bundles ───┘
                                from Inertia server (via Proxy/Mirror)
```

### Components

- **`rpc.rs`** — `RpcClient` (processed commitment) for `getVoteAccounts`, plus a thread that websocket-subscribes to slots and rebroadcasts them. Cheaply `Clone`able.

- **`relayer/`** — the relayer gRPC surface the validator talks to:
  - `tpu.rs` (`Tpu`): binds two QUIC sockets via `solana_streamer`'s `spawn_stake_wighted_qos_server`, pipes into a `SigVerifyStage` through a disabled `BankingTracer` channel. Thread counts are derived from core count. Exposes `Receiver<BankingPacketBatch>` of verified packets. Connection/stream limits are the `const`s at the top of `Tpu`.
  - `staked_nodes_updater_service.rs`: refreshes the pubkey→stake map from `getVoteAccounts` (~every 60s) so the QoS server can stake-weight connections; merges in `--staked-nodes-overrides`.
  - `forwarder.rs` (`Forwarder` / `ForwarderService`): implements the `Relayer` gRPC service. **Max 1 active validator subscription.** A single `run_event_loop` (crossbeam `select!`) fans verified packets out to subscribers in batches of `VALIDATOR_PACKET_BATCH_SIZE` (64), emits heartbeats every 100ms, and drops subscribers whose channels close. `get_tpu_configs` advertises `quic_port - 6` (the UDP TPU port convention).
  - `auth/`: Jito challenge-response auth. `service.rs` issues per-IP challenges (priority queue keyed by age for cheap expiry; DOS mitigation), verifies an **ed25519**-signed challenge, then mints **RS256 JWT** access/refresh tokens signed with the PEM signing key. `interceptor.rs` (`AuthInterceptor`) verifies the Bearer JWT on `Relayer` calls and injects the client `Pubkey` into request extensions. `ValidatorAutherImpl::is_authorized` currently returns `true` for everyone — this is the gate to tighten if access control is needed.

- **`blockengine/`** (`service.rs`) — gRPC proxy implementing both `BlockEngineValidator` and `AuthService`. `subscribe_packets`/`subscribe_bundles` merge **two** sources into the stream returned to the validator: the upstream Jito stream (`pump_upstream`) and a local `from_proxy` broadcast (packets/bundles injected from the Inertia server). If the validator is `PermissionDenied` upstream, it degrades to **proxy-only** instead of erroring. `get_block_engine_endpoints` rewrites endpoint URLs to point back at this local proxy. Upstream clients are pooled by peer `SocketAddr`. Auth methods are pass-through to upstream Jito.

- **`proxy/`** — the glue and the Inertia mirror:
  - `mod.rs` (`Proxy`): applies a fixed **50ms `PACKET_DELAY`** to relayer batches and block-engine packets/bundles before forwarding (delay queues: a std-thread for relayer `BankingPacketBatch`es, the generic async `delay_forward` for broadcast streams). Also mirrors a copy of everything to the Inertia server as it passes through. `SOURCE_RELAYER`/`SOURCE_BLOCKENGINE` tag the mirror frames.
  - `mirror.rs` (`Mirror`): QUIC client to the Inertia server with a **pinned self-signed cert** (`--inertia-cert-sha256`, verified by `PinnedServerCertVerifier`). Outbound packets go as QUIC **datagrams** (`source byte | u64 LE nanos | data`); inbound bundles arrive on uni streams as length-prefixed protobuf and are injected into `bundle_from_proxy`; outbound bundles are mirrored on a uni stream. Auto-reconnects with capped exponential backoff and periodically logs dropped-datagram stats.

- **`protos/`** — Jito `.proto` definitions compiled into modules in `mod.rs` (`tonic::include_proto!`). `convert.rs` maps `solana_perf` packets → proto packets. Note there are **two distinct `SubscribePacketsResponse` types**: `protos::relayer::*` (relayer→validator) and `protos::block_engine::*` (block-engine proxy) — keep them straight when importing.

## Conventions

- Background work is spawned with **named** threads (`std::thread::Builder::new().name(...)`) — match this so logs/`tokio-console` stay legible.
- Channel capacities and timeouts are module-level `const`s near the top of each `impl` (e.g. `Tpu::TPU_QUEUE_CAPACITY`, `Blockengine::BLOCKENGINE_CHANNEL_LIMIT`, `Mirror::*`). Tune there, not inline.
- Setup-time failures (`expect`/`unwrap`/`panic!`) are intentional — invalid config or unreachable dependencies should crash at startup. Steady-state errors are logged (`warn!`/`error!`) and recovered (reconnect, drop subscriber), not panicked.