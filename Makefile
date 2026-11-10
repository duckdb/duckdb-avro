# Builds the avro extension for the DuckDB build named in DUCKDB_PIN and runs its tests on that build.
#
#   make                 cargo build --release + metadata footer -> build/avro.duckdb_extension (needs `make venv` once)
#   make venv            a Python virtualenv with the pinned duckdb wheel (venv/)
#   make test            test/sql (including the generated test/sql/corpus) through scripts/run_sqllogic.py on the
#                        pinned wheel (cwd = repository root, the tests name their fixtures relative to it)
#   make unittest DUCKDB=<duckdb binary of the same build>
#                        the same tests through DuckDB's own C++ sqllogictest runner (test/unittest next to the binary);
#                        the build's own extensions (icu, ...) are linked next to avro so that `require icu` autoloads
#   make clippy          cargo clippy + cargo fmt --check
#   make wasm_eh         the DuckDB-Wasm (wasm_eh) build -> build/wasm_eh/avro.duckdb_extension.wasm; needs emscripten on
#                        PATH (`source <emsdk>/emsdk_env.sh`, extension-ci-tools pins 6.0.9) and the rust target
#                        wasm32-unknown-emscripten (`rustup target add wasm32-unknown-emscripten`). `make wasm` is an alias.
#   make clean
#
# DuckDB-Wasm: a loadable extension is an emscripten side module (what DuckDB's build_loadable_extension does with
# `emcc -O3 -sSIDE_MODULE=2` and what .cargo/config.toml passes for the wasm32-unknown-emscripten target), stamped with
# the same footer as a native build but platform wasm_eh. Only wasm_eh: Rust's prebuilt std for emscripten carries
# wasm exception-handling opcodes (its unwinding), which the wasm_mvp engine does not accept, and wasm_threads needs a
# std built with shared memory and atomics (-Zbuild-std).
#
# Footer tag: a C_STRUCT_UNSTABLE extension is tied to one engine build by the version tag in its footer
# (ExtensionHelper::GetVersionDirectoryName: the library version unless it contains "-dev", then the source id). The
# Python wheel of a pinned build reports an alpha version (v2.0.1-alpha54043), a `make release` of the same commit a
# dev version (v2.0.0-dev89497, source id 29b26cb106), so the two want different tags for the same binary: `make`
# stamps build/avro.duckdb_extension for the wheel, `make unittest` stamps a copy for the given binary.
#
# How `make unittest` gets `require avro` to load the extension: with LOCAL_EXTENSION_REPO set, DuckDB's runner
# (test/sqlite/sqllogic_test_runner.cpp, LoadDatabase) enables autoload_known_extensions and
# autoinstall_known_extensions with that directory as autoinstall_extension_repository; `require avro` then counts as
# PRESENT without loading (avro is in AUTOLOADABLE_EXTENSIONS of extension_entries.hpp) and the first read_avro /
# avro_metadata / COPY ... (FORMAT avro) autoinstalls and loads <repo>/<version tag>/<platform>/avro.duckdb_extension.
# The target lays the stamped copy out in build/repository that way. Autoinstall requires the engine to know the
# function: a function the entries do not list would need an explicit LOAD in the test.
#
# Re-pinning: see docs/UPDATING.md.
include DUCKDB_PIN
PYTHON ?= $(CURDIR)/venv/bin/python
EXT_VERSION := v$(shell grep -m1 '^version' Cargo.toml | sed 's/.*"\(.*\)"/\1/')
# the version tag the footer carries for the wheel: the release tag or alpha version, else the source id of a dev build
FOOTER_VERSION ?= $(shell $(PYTHON) -c "import duckdb; v, s = duckdb.sql('SELECT library_version, source_id FROM pragma_version()').fetchall()[0]; print(s if '-dev' in v else v)" 2>/dev/null)
PLATFORM ?= $(shell $(PYTHON) -c "import duckdb; print(duckdb.sql('PRAGMA platform').fetchall()[0][0])" 2>/dev/null)
RUST_SOURCES := Cargo.toml $(wildcard src/*.rs src/*/*.rs crates/*/Cargo.toml crates/*/src/*.rs crates/*/src/*/*.rs)

all: build/avro.duckdb_extension

venv:
	python3 -m venv venv
	venv/bin/pip install -q "duckdb==$(DUCKDB_WHEEL)"

target/release/libavro.so: $(RUST_SOURCES)
	cargo build --release

build/avro.duckdb_extension: target/release/libavro.so
	@test -n "$(FOOTER_VERSION)" || (echo "no duckdb Python package in $(PYTHON): run make venv (or set PYTHON=)" && exit 1)
	mkdir -p build
	cp $< $@
	python3 -I scripts/append_extension_metadata.py --extension $@ --platform $(PLATFORM) --abi-type C_STRUCT_UNSTABLE --version "$(FOOTER_VERSION)" --extension-version $(EXT_VERSION)

test: build/avro.duckdb_extension
	AVRO_EXTENSION=$(abspath build/avro.duckdb_extension) $(PYTHON) -I scripts/run_sqllogic.py test/sql/*.test test/sql/copy/*.test test/sql/copy/*.test_slow test/sql/corpus/*.test

# the C++ runner: the binary's own version tag and platform name the repository directory the stamped copy goes into
DUCKDB ?=
UNITTEST ?= $(dir $(DUCKDB))test/unittest
TEST_CONFIG ?=
TESTS ?= test/sql/*
DUCKDB_TAG = $(shell $(DUCKDB) -init /dev/null -csv -noheader -c "SELECT CASE WHEN library_version LIKE '%-dev%' THEN source_id ELSE library_version END FROM pragma_version()" 2>/dev/null)
DUCKDB_PLATFORM = $(shell $(DUCKDB) -init /dev/null -csv -noheader -c "PRAGMA platform" 2>/dev/null)
UNITTEST_EXT = build/repository/$(DUCKDB_TAG)/$(DUCKDB_PLATFORM)/avro.duckdb_extension
# the extensions the engine build produced next to the binary (icu, json, parquet, ...), which a test's `require icu`
# autoinstalls from the same repository directory
DUCKDB_REPOSITORY = $(dir $(DUCKDB))repository/$(DUCKDB_TAG)/$(DUCKDB_PLATFORM)
unittest: target/release/libavro.so
	@test -n "$(DUCKDB)" || (echo "usage: make unittest DUCKDB=<duckdb binary>" && exit 1)
	@test -n "$(DUCKDB_TAG)" || (echo "$(DUCKDB) did not report its version" && exit 1)
	mkdir -p $(dir $(UNITTEST_EXT))
	@for ext in $(wildcard $(DUCKDB_REPOSITORY)/*.duckdb_extension); do ln -sf $$ext $(dir $(UNITTEST_EXT)); done
	cp $< $(UNITTEST_EXT)
	python3 -I scripts/append_extension_metadata.py --extension $(UNITTEST_EXT) --platform $(DUCKDB_PLATFORM) --abi-type C_STRUCT_UNSTABLE --version "$(DUCKDB_TAG)" --extension-version $(EXT_VERSION)
	LOCAL_EXTENSION_REPO=$(abspath build/repository) $(UNITTEST) $(if $(TEST_CONFIG),--test-config $(abspath $(TEST_CONFIG))) --test-dir $(CURDIR) "$(TESTS)"

clippy:
	cargo clippy --release --all-targets --workspace
	cargo fmt --check

# DuckDB-Wasm: the cargo `wasm` profile (Cargo.toml) on the emscripten target; rustc links through emcc, so emscripten
# has to be on PATH. The same FOOTER_VERSION mechanism as the native build names the engine build the module is for.
WASM_MODULE := target/wasm32-unknown-emscripten/wasm/avro.wasm
wasm: wasm_eh
wasm_eh: build/wasm_eh/avro.duckdb_extension.wasm

$(WASM_MODULE): $(RUST_SOURCES) .cargo/config.toml
	@command -v emcc >/dev/null || (echo "emcc not on PATH: source <emsdk>/emsdk_env.sh (emscripten 6.0.9, the version extension-ci-tools uses)" && exit 1)
	@rustup target list --installed 2>/dev/null | grep -q '^wasm32-unknown-emscripten$$' || (echo "rust target missing: rustup target add wasm32-unknown-emscripten" && exit 1)
	cargo build --profile wasm --target wasm32-unknown-emscripten --lib -p duckdb-avro

build/wasm_eh/avro.duckdb_extension.wasm: $(WASM_MODULE)
	@test -n "$(FOOTER_VERSION)" || (echo "no duckdb Python package in $(PYTHON): run make venv (or set PYTHON= / FOOTER_VERSION=)" && exit 1)
	mkdir -p build/wasm_eh
	cp $< $@
	python3 -I scripts/append_extension_metadata.py --extension $@ --platform wasm_eh --abi-type C_STRUCT_UNSTABLE --version "$(FOOTER_VERSION)" --extension-version $(EXT_VERSION)

clean:
	cargo clean
	$(RM) -r build

.PHONY: all venv test unittest clippy wasm wasm_eh clean
