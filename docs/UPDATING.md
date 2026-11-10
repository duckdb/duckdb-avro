# Extension updating
The extension targets one exact DuckDB release, `TARGET_DUCKDB_VERSION` in the `Makefile`. When a new DuckDB is
released, update it as follows:

- Bump `duckdb-neo` (the `duckdb-neo` dependency in the root `Cargo.toml`) to a revision that targets the new release,
  and run `cargo update -p duckdb-neo`.
- Set `TARGET_DUCKDB_VERSION` in the `Makefile`, the `duckdb_version` inputs in
  `.github/workflows/MainDistributionPipeline.yml`, and the `api_version` of the entrypoints in
  `crates/native/src/lib.rs` and `crates/wasm/src/lib.rs` to the new release.
- Bump `./extension-ci-tools` to the commit corresponding to the new release.

# API changes
This extension is written in Rust against [duckdb-neo](https://github.com/duckdb/duckdb-rs/tree/main/crates/duckdb-neo),
the Rust wrapper of DuckDB's V2 C API. It contains no C or C++ code and does not use DuckDB's internal C++ API.

Some of the parts of the V2 C API it relies on are still marked as *unstable*: the multi-file function behind
`read_avro`, the per-block batch claiming of its scan, the field ids and metadata it reports for a file, and the
statistics of `COPY ... TO`. Because of this, the extension is built with the `C_STRUCT_UNSTABLE` ABI
(`USE_UNSTABLE_C_API=1`), which pins a build to the exact DuckDB version it was built against.
