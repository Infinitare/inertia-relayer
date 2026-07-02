cargo build --release
sudo systemctl stop inertia-relayer
sudo cp target/release/inertia-relayer /etc/inertia-relayer/
sudo systemctl start inertia-relayer