#!/usr/bin/env bash
# Builds the C++ implementation of the extension (the last commit before the Rust rewrite, by default) as a release
# loadable extension, to benchmark against. Needs CMake, Ninja, a C++ toolchain and vcpkg (VCPKG_ROOT, default ~/vcpkg).
#
# usage: benchmark/build_baseline.sh [git ref] [target directory]
set -euo pipefail

REF=${1:-b7b157f}
TARGET=${2:-$HOME/duckdb-avro-cpp}
VCPKG_ROOT=${VCPKG_ROOT:-$HOME/vcpkg}
REPO=$(cd "$(dirname "$0")/.." && pwd)
DUCKDB_VERSION=$(sed -n 's/^TARGET_DUCKDB_VERSION=//p' "$REPO/Makefile")

if [ ! -f "$VCPKG_ROOT/scripts/buildsystems/vcpkg.cmake" ]; then
    echo "vcpkg not found at $VCPKG_ROOT; set VCPKG_ROOT" >&2
    exit 1
fi

if [ ! -d "$TARGET" ]; then
    mkdir -p "$TARGET"
    git -C "$REPO" archive "$REF" | tar -x -C "$TARGET"
    # git archive leaves the submodules empty; check them out at the commits the ref pins
    git -C "$REPO" ls-tree -r "$REF" | awk '$2 == "commit" { print $3, $4 }' | while read -r commit path; do
        url=$(git -C "$REPO" show "$REF:.gitmodules" | git config -f /dev/stdin --get "submodule.$path.url")
        rm -rf "${TARGET:?}/$path"
        git clone -q "$url" "$TARGET/$path"
        git -C "$TARGET/$path" checkout -q "$commit"
    done
fi

# Every build has to load into the same CLI, so build against the DuckDB release the Rust build targets rather than
# the commit the ref pins.
git -C "$TARGET/duckdb" rev-parse -q --verify "$DUCKDB_VERSION^{commit}" > /dev/null ||
    git -C "$TARGET/duckdb" fetch -q origin tag "$DUCKDB_VERSION"
git -C "$TARGET/duckdb" checkout -q "$DUCKDB_VERSION"

cd "$TARGET"
VCPKG_TOOLCHAIN_PATH="$VCPKG_ROOT/scripts/buildsystems/vcpkg.cmake" GEN=ninja \
    CMAKE_BUILD_PARALLEL_LEVEL=${CMAKE_BUILD_PARALLEL_LEVEL:-$(sysctl -n hw.ncpu 2>/dev/null || nproc)} make release

# The build stamps the DuckDB commit as the version it loads into, which release CLIs (e.g. v2.0.1) reject. Stamp the
# release instead: the commit is the release's, so the binary is the same.
EXTENSION=build/release/extension/avro/avro.duckdb_extension
PLATFORM=$(cat build/release/duckdb_platform_out)
head -c $(($(wc -c < "$EXTENSION") - 512)) "$EXTENSION" > build/release/avro.no_metadata
python3 extension-ci-tools/scripts/append_extension_metadata.py -l build/release/avro.no_metadata \
    -o build/release/avro.duckdb_extension -n avro -dv "$DUCKDB_VERSION" -ev "$REF" -p "$PLATFORM" \
    --abi-type C_STRUCT_UNSTABLE
echo "baseline: $TARGET/build/release/avro.duckdb_extension"
