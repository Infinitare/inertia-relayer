# How to Setup

### If you have any Questions, feel free to contact us on Telegram
The relayer is designed to connect to exactly one Validator. We recommend installing it on the same machine as the Validator to ensure they share the same IP.

Before installation, make sure the following tools are installed on your machine:
- Rust
- Git
- Solana CLI (or another way to create a keypair)

Next, please install some Dependencies
```bash
sudo apt-get install \
    build-essential \
    pkg-config \
    libudev-dev llvm libclang-dev \
    libssl-dev \
    protobuf-compiler -y
```

Create a folder for related files
```bash
sudo mkdir /etc/inertia-relayer
```

Create a Solana Keypair - this will be used to verify against our Server
The Keypair does NOT have to be verified by Jito
```bash
solana-keygen new --no-bip39-passphrase --outfile /etc/inertia-relayer/keypair.json
```

Clone this repository
```bash
git clone https://github.com/Infinitare/inertia-relayer.git
```

Copy the service file to the systemd folder
```bash
sudo cp inertia-relayer/inertia-relayer.service /etc/systemd/system/
```

Edit the service file using
```bash
sudo nano /etc/systemd/system/inertia-relayer.service
```

Here you have to change the following lines:
- BLOCKENGINE_URL - fill in the Jito Block Engine URL that is closest to your Validator's location, you can find it [here](https://docs.jito.wtf/lowlatencytxnsend/#api)
- INERTIA_IP:PORT - you'll get this from us
- INERTIA_CERT_SHA256_64_HEX - you'll get this from us

opt. flags you may have to set:
- --rpc-server - if you don't use the default ip / port (127.0.0.1:8899) at your Validator
- --websocket-server - if you don't use the default ip / port (127.0.0.1:8900) at your Validator

Save the file (ctrl + s) and exit (ctrl + x).
Reload the systemd daemon to apply the changes
```bash
sudo systemctl daemon-reload
```

Run the update script to get everything set and running
```
cd inertia-relayer && ./update.sh
```

This will also print a command to see the logs

If everything works, you can enable the Service to start on boot
```bash
sudo systemctl enable inertia-relayer.service
```

Make sure you have whitelisted the following Ports in your Firewall
```bash
sudo ufw allow 11227
sudo ufw allow 11228
sudo ufw allow 11229
```

If you're running the Relayer on a different Server, you also need to allow the following Ports
```bash
sudo ufw allow 11225
sudo ufw allow 11226
```

Lastly, you have to add the following lines to your Startup Script
```
  --relayer-url http://127.0.0.1:11225/ \
  --block-engine-url "http://127.0.0.1:11226/" \
```

You can either restart your Validator or run the following command to apply the changes
```bash
agave-validator --ledger /mnt/ledger/ set-relayer-config --relayer-url http://127.0.0.1:11225
agave-validator --ledger /mnt/ledger/ set-block-engine-config --block-engine-url "http://127.0.0.1:11226"
```

To see if everything is working fine, you can check if the Relayer received subscriptions from the Validator
```bash
tail /etc/inertia-relayer/relayer.log -n 100000 | grep "Received"
```

If you see any messages then the Blockengine is connected.

To check if the Relayer is connected, you can check your Validator Ports using
```bash
solana gossip
```

If you see, that your Validator is using the Ports 11228 - everything is working fine.
Keep in mind, that it may take a few minutes until it shows in the gossip, so be patient.

### Using with SWQOS
If you want to use the Relayer with SWQOS, please add the same overrides flag you're using for the Validator to the Relayer Service file.
```bash
--staked-nodes-overrides=/etc/swqos/overrides.yml
```

After, point the SWQOS of the RPC Node you want to connect to the Port of the Relayer (11228) and you're set.
