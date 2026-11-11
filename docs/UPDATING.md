# Re-pinning the DuckDB build

The extension speaks the C API v2, which is unstable: the `duckdb_ext_api_v2` function table the engine hands to the
entrypoint has the member order of the engine's `src/include/duckdb_extension_v2.h`, and the `C_STRUCT_UNSTABLE`
metadata footer ties a built `avro.duckdb_extension` to one engine version tag. Both follow `DUCKDB_PIN`.

To move to another DuckDB build:

1. Edit `DUCKDB_PIN`: `DUCKDB_WHEEL` (the version of the `duckdb` Python wheel of that build: a release such as
   `2.0.0`, or a nightly `X.Y.Z.devNNNNNNNNNN`), `DUCKDB_VERSION` (its `library_version`) and `DUCKDB_SOURCE_ID` (its
   `source_id`, both from `SELECT library_version, source_id FROM pragma_version()`).
2. Fetch that build's header: `git -C <duckdb checkout> show <tag or source id>:src/include/duckdb_extension_v2.h > /tmp/duckdb_extension_v2.h`.
3. Reorder the function table and compare the signatures:
   `python3 -I scripts/pin_api_table.py /tmp/duckdb_extension_v2.h --check-signatures`.
   - The struct fields and the name tables take the header's member order. The script prints how many entries follow
     the header and how many it appended; it stops when the header names a function the Rust struct lacks (then the
     bindings in `crates/duckdb-capi-sys/src/lib.rs` need the new entry).
   - Entries the header lacks stay behind the header's. The band from `duckdb_v2_library_api_table_size` on keeps
     its order; entries that stood before it (functions of a newer DuckDB than the one being pinned, e.g. the 21
     `*_stats_*` entries v2.0.0 does not have) move to the very end, behind that band. `UPSTREAM_API_TABLE_SIZE` is
     the offset of `duckdb_v2_library_api_table_size`, so it stays exactly the pinned header's table size, and the
     moved entries are `None` on the pinned engine (their wrappers report a missing entry).
   - `--check-signatures` compares the return type and every parameter type of each header entry with the Rust
     field of the same name and lists the ones that differ (exit status 1). The order is rewritten either way; fix a
     listed binding in `crates/duckdb-capi-sys/src/lib.rs` and its wrapper in `crates/duckdb-ext-api` by hand (for
     v2.0.0: `duckdb_v2_parse_sql` has no `apply_invalidation_policy` parameter) and run the script again until it
     reports all signatures equal.
4. Rebuild the virtualenv on the new wheel: delete `venv/` and run `make venv`.
5. Rebuild and test: `make clean && make && make test`, and `make unittest DUCKDB=<duckdb binary of that build>`
   where a source build of it is at hand.
6. Regenerate the corpus tests (`test/corpus/README.md`, "Regenerating"): their expected output follows the pinned
   build's rendering of values. The diff should be rendering only; the generator's cross-check against fastavro
   must still pass.
7. Replace the old version, tag and source id where the text names them: `README.md` ("Building"), the `Makefile`
   header, this file.

Also check `crates/duckdb-capi-sys/src/lib.rs` against the build's `src/include/duckdb_v2.h` for changed struct
layouts (e.g. `duckdb_v2_vector_view`, see the comment in `crates/duckdb-ext-api/src/vector.rs`): the signature check
compares type names, not the layout of the types behind them.
