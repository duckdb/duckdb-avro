# Re-pinning the DuckDB build

The extension speaks the C API v2, which is unstable: the `duckdb_ext_api_v2` function table the engine hands to the
entrypoint has the member order of the engine's `src/include/duckdb_extension_v2.h`, and the `C_STRUCT_UNSTABLE`
metadata footer ties a built `avro.duckdb_extension` to one engine version tag. Both follow `DUCKDB_PIN`.

To move to another DuckDB build:

1. Edit `DUCKDB_PIN`: `DUCKDB_WHEEL` (the version of the `duckdb` Python wheel of that build, e.g. a nightly
   `2.0.1.devNNNNNNNNNN`), `DUCKDB_VERSION` (its `library_version`) and `DUCKDB_SOURCE_ID` (its `source_id`, both
   from `SELECT library_version, source_id FROM pragma_version()`).
2. Fetch that build's header: `git -C <duckdb checkout> show <source id>:src/include/duckdb_extension_v2.h > /tmp/duckdb_extension_v2.h`.
3. Reorder the function table: `python3 -I scripts/pin_api_table.py /tmp/duckdb_extension_v2.h`. It prints how many
   entries follow the header; it stops when the header names a function the Rust struct lacks (then the bindings in
   `crates/duckdb-capi-sys/src/lib.rs` need the new entry).
4. Rebuild the virtualenv on the new wheel: delete `venv/` and run `make venv`.
5. Rebuild and test: `make clean && make && make test`.

Also check `crates/duckdb-capi-sys/src/lib.rs` against the build's `src/include/duckdb_v2.h` for changed struct
layouts (e.g. `duckdb_v2_vector_view`, see the comment in `crates/duckdb-ext-api/src/vector.rs`).
