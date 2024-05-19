#!/usr/bin/env sh
export PATH=/home/ubuntu/.cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin

echo "Starting service..."

RUST_LOG="slot0=info" cargo run --release