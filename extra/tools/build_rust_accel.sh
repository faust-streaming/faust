#!/bin/sh
# Build the evaluation-only Rust accelerator in faust/_rust/.
#
# This crate is deliberately NOT wired into setup.py: see
# docs/proposals/rust-acceleration.md for why (PEP 518 has no conditional
# build-requires, so wiring it up would make setuptools-rust a mandatory
# build dependency for everyone).  This script exists so the numbers in that
# document can be re-checked without changing Faust's build.
#
# Usage:
#     extra/tools/build_rust_accel.sh              # abi3 (what a real PR would ship)
#     extra/tools/build_rust_accel.sh --no-abi3    # adds the macro-based variants
#
# Then:
#     python extra/tools/bench_accel_offsets.py
set -e

CRATE_DIR="$(dirname "$0")/../../faust/_rust"
cd "$CRATE_DIR"

# Remove any previous build FIRST: if the compile below fails, a stale module
# left in place would be picked up by the benchmark and silently reported as
# the build you asked for.
rm -f _accel.so _accel.abi3.so

if [ "$1" = "--no-abi3" ]; then
    echo "--- building without abi3 (enables the macro-based variants) ---"
    cargo build --release --no-default-features
    OUT="_accel.so"
else
    echo "--- building with abi3-py310 ---"
    cargo build --release
    OUT="_accel.abi3.so"
fi

cp target/release/lib_accel.so "$OUT"
echo "built $CRATE_DIR/$OUT"
