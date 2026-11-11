#!/usr/bin/env bash
# Build the kernel and copy it next to the browser client.
# Needs: rustup target add wasm32-unknown-unknown
set -euo pipefail
cd "$(dirname "$0")"
RUSTFLAGS="-C target-feature=+simd128" cargo build --release --target wasm32-unknown-unknown
cp target/wasm32-unknown-unknown/release/vibegen_kernel.wasm ../server/static/kernel.wasm
ls -l ../server/static/kernel.wasm
