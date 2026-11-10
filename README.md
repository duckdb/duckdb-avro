# avro: reading and writing Apache Avro files with DuckDB

`avro` is a DuckDB extension that reads and writes [Apache Avro](https://avro.apache.org) object container files,
written in Rust against DuckDB's C API v2 and nothing else, so one build loads into any DuckDB of the pinned version
(the CLI with `-unsigned`, the Python package, ...). It is a rewrite of the C++ `duckdb-avro` extension, whose
source is in this repository's history (commit b7b157f, upstream main of 2026-11-04); that extension's sqllogictests and fixture files are
kept unchanged under `test/` as the acceptance suite.

## Functions

| function | result |
|---|---|
| `read_avro(files, ...)` | the rows of one or more files: a path, a glob, a list of paths, or a list of file structs |
| `read_single_avro_file(file)` | the rows of one file (the function `read_avro` is built on) |
| `avro_metadata(path)` | the key-value entries of a file's header (`avro.schema`, `avro.codec`, anything the writer added); a value that is not UTF-8 is rendered with replacement characters |
| `COPY ... TO 'x.avro' (FORMAT avro, ...)` | writes a query result as an Avro file |

```sql
LOAD 'build/avro.duckdb_extension';
FROM read_avro('test/userdata1.avro');
FROM read_avro('http://blob.duckdb.org/data/userdata1.avro');
FROM read_avro('s3://my-bucket/events_*.avro', filename=true);
FROM read_avro(['a.avro', 'b.avro'], union_by_name=true);
SELECT * FROM avro_metadata('test/bigdata.avro');
COPY (SELECT 42 AS answer) TO 'out.avro' (FORMAT avro, CODEC 'deflate');
```

Any DuckDB file system works: local paths, `http://`, `s3://` through httpfs with the usual secrets. Files are read
through the engine's external file cache. There is no replacement scan: `FROM 'x.avro'` does not work, use
`read_avro('x.avro')` (as with the C++ extension).

### Several files

`read_avro` is DuckDB's multi-file layer over `read_single_avro_file`, so its named parameters are the layer's:
`filename` (add a column with the file's path), `union_by_name`, `hive_partitioning`, `schema`. By default the first
file decides the columns and types. `union_by_name=true` opens every file and combines the columns by name, a column
a file lacks reading as `NULL`. The `schema` argument pins the columns without opening a file: a `MAP` from column
name to its name, type and the default a file without the column reads. Each file is mapped onto that schema by name
(case-insensitively), a column of another type is cast, and columns not in the map are not read. With `INTEGER` keys
the files are mapped by Avro field id (the `field-id` attribute Iceberg writes) instead of by name.

```sql
FROM read_avro('events_*.avro', schema=MAP {
    'id': {name: 'id', type: 'BIGINT', default_value: NULL},
    'added_later': {name: 'added_later', type: 'VARCHAR', default_value: 'n/a'}
});
```

### COPY options

| option | meaning |
|---|---|
| `ROOT_NAME 'name'` | the name of the top-level record (default `root`) |
| `FIELD_IDS {...}` | Iceberg field ids: `{col: 42, nested: {__duckdb_field_id: 43, __duckdb_nullable: false, child: 44}}` (LIST children are keyed `list`, MAP children `key` and `value`); `__duckdb_nullable: false` writes a field without the null union |
| `SANITIZE_FIELD_NAMES true` | rewrite column and nested struct field names into valid Avro identifiers (below) |
| `METADATA {key: 'value', ...}` | extra string entries for the file header |
| `CODEC 'name'` | block compression: `null` (default), `deflate`, `snappy`, `zstandard`, `bzip2`, `xz`, `lzma` |

Option names are case-insensitive; an unknown option is an error. A file gets one block per input chunk, so a
written file can be read back in parallel.

By default field names must match `[A-Za-z_][A-Za-z0-9_]*`. With `SANITIZE_FIELD_NAMES true` valid ASCII names stay
unchanged, a leading digit gets an underscore (`1col` -> `_1col`) and every other invalid byte becomes `_x` plus its
uppercase hex (`a key` -> `a_x20key`, `a-b` -> `a_x2Db`); non-ASCII names are escaped byte by byte in their UTF-8
encoding, following the convention of Apache Iceberg's `AvroSchemaUtil`. The rewrite applies recursively, including
structs inside lists and maps, but not to map keys or `ROOT_NAME`; `FIELD_IDS` keep using the original names. Two
fields of a record that sanitize to the same name are an error. Names are not restored when reading the file back.

### Codecs

Reading: `null`, `deflate`, `snappy` (with the CRC-32 trailer check), `zstandard`, `bzip2`, `xz` and avro-c's
`lzma` (raw LZMA2 chunks). Writing: the same list. `deflate` is written at level 6, `bzip2` at level 9;
`zstandard` writes zstd frames of the level-1 class (`ruzstd`'s fastest encoder, the only one it implements); `xz`
and `lzma` write valid xz / LZMA2 streams whose chunks are stored uncompressed, because `lzma-rs` has no real
encoder. Readers of every implementation accept all of these.

## Schema conversion

The root of a file is normally a record; its fields become the columns. A root that is a union of null and a record
is treated the same (a NULL root value is a row of NULLs). Any other root is read as one column named after the
type, or `avro_schema` for an unnamed one. Reading:

| Avro | DuckDB |
|---|---|
| `null` | `"NULL"` |
| `boolean`, `int`, `long`, `float`, `double` | `BOOLEAN`, `INTEGER`, `BIGINT`, `FLOAT`, `DOUBLE` |
| `string`, `bytes`, `fixed` | `VARCHAR`, `BLOB`, `BLOB` |
| `int` + `date` | `DATE` |
| `int` + `time-millis`, `long` + `time-micros` | `TIME` |
| `long` + `timestamp-millis` / `timestamp-micros` | `TIMESTAMP`, or `TIMESTAMP WITH TIME ZONE` with `adjust-to-utc: true` |
| `long` + `timestamp-nanos` | `TIMESTAMP_NS`, or `TIMESTAMP_TZ_NS` with `adjust-to-utc: true` |
| `long` + `local-timestamp-millis` / `local-timestamp-micros` | `TIMESTAMP` |
| `long` + `local-timestamp-nanos` | `TIMESTAMP_NS` |
| `string` + `uuid`, `fixed(16)` + `uuid` | `UUID` (the text form is parsed: 36 characters, hex digits of either case; another fixed size is an error) |
| `fixed(12)` + `duration` | `INTERVAL` (months, days, milliseconds; a count beyond 2^31-1 is an Out of Range error) |
| `bytes` or `fixed` + `decimal` | `DECIMAL(precision, scale)`; `BLOB` when the attributes are invalid (no integer precision, precision outside 1..38, scale outside 0..precision) |
| `enum` | `ENUM` of the symbols |
| `record` (or `error`, the protocol error record) | `STRUCT`; a record without fields is `"NULL"` |
| `array` | `LIST`; with `"logicalType": "map"` over key/value records a `MAP(key, value)` (the records may be nullable, as BigQuery's Iceberg manifest exports write them; a null item is skipped) |
| `map` | `MAP(VARCHAR, value)` |
| `union` | null branches dropped; one branch left: that type, nullable; none left: `"NULL"`; several: `UNION(u<i> T, ...)`, `i` the branch index |

Logical types are honoured on primitives only; an array's `map` annotation is the one exception. An unknown logical
type (`char`, `time-interval-micros`, any vendor name) or an invalid one is ignored and the underlying type is read,
as the specification asks. Recursive records are not supported (see the limitations below).

Writing (`COPY ... TO`), as the C++ extension does it:

| DuckDB | Avro |
|---|---|
| `BOOLEAN`, `INTEGER`, `BIGINT`, `FLOAT`, `DOUBLE` | `boolean`, `int`, `long`, `float`, `double` |
| `VARCHAR`, `BLOB`, `"NULL"` | `string`, `bytes`, `null` |
| `DATE`, `TIME` | `int` + `date`, `long` + `time-micros` |
| `TIMESTAMP`, `TIMESTAMP_MS` | `long` + `timestamp-micros`, `long` + `timestamp-millis` |
| `TIMESTAMP WITH TIME ZONE` | `long` + `timestamp-micros`, `adjust-to-utc: true` |
| `TIMESTAMP_NS`, `TIMESTAMP_TZ_NS` | `long` + `timestamp-nanos` (+ `adjust-to-utc`) |
| `UUID` | `fixed(16)` + `uuid` |
| `DECIMAL(P, S)` | `fixed(n)` + `decimal`, `n` the smallest size holding `P` digits in two's complement |
| `STRUCT` | `record` |
| `LIST` | `array` |
| `MAP` | `array` + `"logicalType": "map"` of `record(key, value)` (the Iceberg form, so it reads back as a `MAP`) |
| `ENUM` | `enum` of the symbols in order (named like a record; the symbols must be Avro identifiers) |
| `UNION` | `union` of the member types in member order, `null` first when the field is nullable |

Every field, element, key and value is written as `["null", T]` unless `FIELD_IDS` marks it `__duckdb_nullable:
false`. A `record` or `fixed` is named after its field (`r<id>` with a field id, `record<n>` / `fixed<n>` for array
items); Avro wants those names distinct, so a name a nested field repeats (`{'value': {'x': 1}} AS s1, {'value': {'y':
2}} AS s2`) gets the suffix `_2`, `_3`, ... from the second definition on. Avro unions are positional, so the member
names of a `UNION` are not written and a round trip reads them back as `u<branch index>` (`UNION(a INTEGER, b VARCHAR)`
becomes `UNION(u1 INTEGER, u2 VARCHAR)` after the `null` branch); a union member cannot be another `UNION` or `"NULL"`,
and two members that map to the same unnamed Avro type (two `INTEGER`s, two `LIST`s) are refused since an Avro union
holds every unnamed type once. Other types (`TINYINT` ... `HUGEINT`, unsigned integers, `INTERVAL`, `ARRAY`, ...)
are refused with `Can't convert logical type ... to Avro type`.

## Differences from the C++ extension

- A logical type on an Avro type it does not belong to (`date` on a `string`, say) is an error at bind; the C++
  applied the logical type regardless.
- Recursion is detected by schema node, not by the record's simple name, so two distinct records that share a name
  across namespaces are not mistaken for a recursive type.
- A `decimal` with invalid attributes (a precision above 38, say, or Hive 0.x's `"precision": "5"`) is read as a
  `BLOB` of the underlying type, as the specification asks for an invalid logical type; the C++ fails on it. An
  unknown logical type on an `int` or `long` is ignored like one on a `string` (the C++ fails on the former).
- An unknown `CODEC` is rejected when the `COPY` is bound, not when the first block is written.
- `avro.codec` names are matched case-insensitively.
- `TIMESTAMP_MS` is written with `timestamp-millis`; the C++ writes the millisecond values under `timestamp-micros`
  (upstream bug report pending).
- User `METADATA` keys starting with `avro.` are rejected; avro-c would let them collide with the schema and codec
  entries.
- `time-millis` / `time-micros` values outside 00:00:00..24:00:00 are an Out of Range error instead of an internal
  error.
- An array or map block count the remaining bytes cannot hold (or beyond 2^24 zero-size items) fails fast instead of
  spinning the decoder.

## Limitations

- Recursive record schemas are not supported: DuckDB has no recursive types (`Recursive Avro types not supported`).
- A `decimal` of more than 38 digits is read as the underlying `bytes` / `fixed` `BLOB`, the widest `DECIMAL` being 38.
- An array or map block count of `INT64_MIN` (which has no magnitude) is rejected as corrupt; fastavro reads such an
  array as empty.
- A record with two fields of the same name is rejected, as Java does (fastavro reads the second field).
- A named type defined twice in one schema is rejected even when the two definitions are identical (older Iceberg
  Java manifests have such a `decimal` fixed; iceberg-rust special-cases them).

## Building

Needs a Rust toolchain (edition 2024, `rust-version` 1.99) and Python 3 for the virtualenv and the scripts.

```sh
make venv          # a virtualenv with the pinned duckdb wheel (DUCKDB_PIN)
make               # cargo build --release + the metadata footer -> build/avro.duckdb_extension
make clippy        # cargo clippy --release --all-targets + cargo fmt --check
venv/bin/python -c "import duckdb; c = duckdb.connect(config={'allow_unsigned_extensions': 'true'}); c.sql(\"LOAD 'build/avro.duckdb_extension'\"); print(c.sql(\"FROM read_avro('test/userdata1.avro') SELECT count(*)\").fetchall())"
```

The C API v2 is not stable before DuckDB 3.0: the function table an extension receives is the member order of the
engine's `duckdb_extension_v2.h`, and the `C_STRUCT_UNSTABLE` footer `make` appends names the exact DuckDB build the
extension was built for, which the engine checks at `LOAD`. `DUCKDB_PIN` names that build (a `2.0.1` nightly wheel:
`DUCKDB_WHEEL`, `DUCKDB_VERSION`, `DUCKDB_SOURCE_ID`); `crates/duckdb-capi-sys` follows its header. Loading needs
`allow_unsigned_extensions` (`duckdb -unsigned`).

The footer tag is the engine's library version unless it contains `-dev`, then its source id. The Python wheel of a
pinned build reports an alpha version (`v2.0.1-alpha54043`) while a source build of the same commit reports a dev
version (`v2.0.0-dev89497`, source id `29b26cb106`), so one binary wants two tags: `make` stamps
`build/avro.duckdb_extension` for the wheel, and `make unittest DUCKDB=<duckdb binary built at the pinned commit>`
stamps a copy under `build/repository/<tag>/<platform>/` for that binary and runs DuckDB's own C++ sqllogictest
runner (`test/unittest` next to the binary) with `LOCAL_EXTENSION_REPO` pointing there, so `require avro` autoloads
it; the binary's own extensions (icu, ...) are linked next to it for the tests that `require` them. Another DuckDB
can be targeted with `FOOTER_VERSION=<version tag or source id> make`.

Moving the pin to another DuckDB build is described in [`docs/UPDATING.md`](docs/UPDATING.md): edit `DUCKDB_PIN`,
reorder the function table with `scripts/pin_api_table.py` against that build's header, rebuild the virtualenv.

### DuckDB-Wasm

`make wasm_eh` (alias `make wasm`) builds the extension for DuckDB-Wasm as an emscripten side module,
`build/wasm_eh/avro.duckdb_extension.wasm`, stamped for platform `wasm_eh`. It needs emscripten on `PATH`
(`source <emsdk>/emsdk_env.sh`; extension-ci-tools pins 6.0.9) and the `wasm32-unknown-emscripten` rust target
(`rustup target add wasm32-unknown-emscripten`). The module is built with the `wasm` cargo profile (fat LTO, one codegen
unit, stripped) and the side-module flags in `.cargo/config.toml`; it imports only libc and the dynamic-linker symbols
every DuckDB-Wasm main module exports. Only `wasm_eh`: Rust's prebuilt std for emscripten uses wasm exception handling
for unwinding, which the `wasm_mvp` engine does not accept, and `wasm_threads` would need a std built with shared memory
and atomics.

## Layout

- `crates/avro-core`: the Avro implementation without any DuckDB dependency: JSON schema parser and writer (the
  compact form avro-c emits), binary decoder and encoder, object container reader and writer, the codecs
  (`miniz_oxide`, `snap`, `ruzstd`, `bzip2` on `libbz2-rs-sys`, `lzma-rs`; no C code). `tools/oracle_check.py`
  cross-checks its dump of a file against `fastavro`.
- `crates/duckdb-capi-sys`: the C API v2 function table and types; `crates/duckdb-ext-api`: a safe wrapper over it
  (standard library only).
- `src/`: the extension. `reader/` (`read_single_avro_file`, `read_avro`; `schema.rs` is the Avro-to-DuckDB
  conversion, `node.rs` the per-column decoders), `metadata.rs` (`avro_metadata`), `copy/` (the `COPY TO` writer;
  `schema.rs` is the DuckDB-to-Avro conversion, `field_ids.rs` the `FIELD_IDS` option).
- `test/`: the upstream suite, `test/sql/*.test` and `test/sql/copy/*.test`, with the fixture files; `test/corpus/`
  the real-world files and `test/sql/corpus/` the tests generated over them.
- `scripts/`: `pin_api_table.py` (re-pinning), `append_extension_metadata.py` (the footer), `run_sqllogic.py` (the
  test runner), `gen_corpus_tests.py` with `corpus_canon.py` (the corpus tests), and the generators of the independent
  fixtures (`create_decimal_bytes.py`, `create_timestamp_*.py`, `create_malformed.py`: the hand-built malformed files,
  `create_corpus_fixtures.py`: the `test/corpus` reproductions of the real-world corpus findings; all stdlib only).

## Tests

`make test` runs all 96 test files (`test/sql/*.test`, `test/sql/copy/*.test` and `*.test_slow`, and the generated
`test/sql/corpus/*.test`) with `scripts/run_sqllogic.py`, a sqllogictest runner on the pinned Python wheel that
follows DuckDB's C++ runner for the directives the tests use. `make unittest DUCKDB=<binary>` runs the same files
through the C++ runner (above); `TESTS=` selects a subset. `cargo test --release` runs the unit tests of the crates
(codec round trips, schema JSON, the decoder).

`make kani` runs the Kani proof harnesses; see [Formal verification with Kani](#formal-verification-with-kani).

`test/corpus/<source>/` holds 1293 real-world Avro files (at most 64 KiB each, permissive licenses only; provenance
in `test/corpus/MANIFEST.tsv`, attribution in `test/corpus/LICENSES.md`) copied from the test data of 52 public
sources, and `scripts/gen_corpus_tests.py` generates one test file per source over them: the row count as fastavro
reads it, the schema and content as this reader produces them (explicit rows up to 100 rows, `SUMMARIZE` above,
cross-checked row by row against fastavro's canonical dump before they are written down), a writer round trip
through `COPY ... (FORMAT avro)` and `EXCEPT ALL`, and the error message for the files the reader or writer refuses.
`test/corpus/README.md` has the details and the regeneration procedure.

## Formal verification with Kani

[Kani](https://model-checking.github.io/kani/) model-checks the pure, engine-free pieces of the code: each harness
fixes an input bound and CBMC then checks every input within it (all values of a fixed-width type, all byte strings up
to the bound) for panics, arithmetic overflow, out-of-bounds accesses and the stated property, so the result is a
proof over that bound rather than a sample. Proven so far (bounds in parentheses):

- Avro binary layer (`avro-core`): zigzag encode/decode round trips (all i64/u64); `read_varint`/`read_long`/`read_int`
  against a 128-bit reference (slices <= 11 bytes, incl. the 10-byte wrap of the tenth byte); `read_block_count` never
  admits more items than the remaining bytes feed (slices <= 12 bytes, every item size); the primitive readers never
  panic (`boolean` 2 bytes, `float`/`double`/`fixed`/`bytes` 16, enum and union index 11); `read_string` accepts exactly
  well-formed UTF-8 (payloads <= 7 bytes); `skip` of every leaf kind lands where the typed reader lands and fails only
  where it fails (slices <= 12 bytes, every `fixed` size and enum count); encoder -> decoder round trips of `long`,
  `int`, `boolean`, `float`, `double`, `bytes`/`fixed` (<= 8 bytes), enum/union indices and block counts (the sized
  form behind `kani-heavy`); `read_raw` / `read_fixed` take exactly `n` bytes or nothing and leave the position where
  they say (slices <= 16 bytes, every `n` and start); the encoder's `long` is the spec's zigzag varint byte by byte
  (all i64, no over-long form) and `boolean` / `float` / `double` / the block end the spec's bytes; the container
  writer's block prefix reads back as its row count and payload size (all counts and sizes); the CRC-32 table and
  `crc32` of 0-2 bytes against the bitwise definition.
- Schema: `min_encoded_size` of every leaf kind (`fixed` <= 32 bytes); `Name::parse`/`fullname` split at the last dot
  and invert each other except for a lone leading dot (strings <= 12 bytes, `kani-heavy`).
- Codec names: `Codec::from_name` accepts exactly the seven names case-insensitively, no trimming (ASCII <= 10 bytes).
- JSON: the number grammar `-?(0|[1-9][0-9]*)(.[0-9]+)?([eE][+-]?[0-9]+)?` is accepted exactly (literals <= 6 bytes),
  integer literals against a 128-bit reference (1-4 digits every length, and the 19-digit `i64` boundary), written
  integers parse back (|value| < 10^4, `kani-heavy`).
- Value conversions (the extension): decimal bytes at every storage width (<= 16 bytes), `min_bytes_required_for_decimal`
  and the writer -> reader decimal round trip (all precisions, all values), `timestamp-millis`/`time-millis`/
  `time-micros` scaling and ranges (all i32/i64), `duration` (all 12-byte patterns), UUID storage form both ways and
  `parse_uuid` (strings <= 40 bytes); the reader's union member names `u0..u255` and the u8 tag, the lenient `decimal`
  attribute rule (all optional i64 pairs), ENUM/DECIMAL storage widths hold every value.
- Type mapping: the reader's `logicalType` parser on every name the spec defines (and unknown, miscased, empty
  names) for every `adjust-to-utc` and attribute pair; its primitive decision table on every Avro primitive (every
  `fixed` size) times every annotation against a spec table written by logical type (type, decoding shape and error
  class); the writer's type tables over every type id (each row of its table, every other id refused, the temporal
  logical types and `adjust-to-utc`, the named types, and the encoder's plan writing the Avro type the schema
  declares); every primitive type the writer accepts reads back as itself through the two halves (TIMESTAMP_MS as
  TIMESTAMP; DECIMAL at every precision and scale, with the writer's `fixed` size and the reader's storage width);
  the writer's union branch index (`null` first when nullable) against the reader's union layout for every tag
  (<= 8 members) and the layout's numbering of every null / non-null pattern (<= 8 branches); the DECIMAL storage
  types of writer and reader agree at every width.
- Writer: the identifier check against the grammar (strings <= 8 bytes).
- C API wrapper (`duckdb-ext-api`): validity mask bit arithmetic (2048 rows: exactly bit `row` changes), both
  `string_t` forms (<= 64 bytes), the derived entry count of a selection view (<= 8 indices).

Running: `make kani` (needs `cargo install --locked kani-verifier && cargo kani setup`), or per package `cargo kani -Z
stubbing -p avro-core` / `-p duckdb-ext-api` / `-p duckdb-avro`; the `kani-heavy` features of `avro-core` and the
extension enable the four harnesses that need more than a 16 GiB runner holds or run for several minutes (the sized
block count, the 12-byte names, the written integers, and `String::from_utf8_lossy` of <= 4 bytes, what
`avro_metadata` renders a non-text value with). CI runs everything else, about 22 minutes. The harnesses are the
`#[cfg(kani)] mod verification` blocks next to
the code (`crates/avro-core/src/{decode,encode,crc32,schema,json,codec,container}.rs`,
`crates/duckdb-ext-api/src/verification.rs`, `src/verification.rs`); under `cfg(kani)` error messages are the bare
format strings (`core::fmt` is out of CBMC's reach, no property is about message text) and `core`'s float parsing is
stubbed where a harness says so.

Deliberately not covered: the compression codecs (third-party crates, far beyond bounded model checking); the
recursive JSON value parser and `Schema::parse` (CBMC inlines recursion once per call site per unwinding, and every
level allocates growable strings and vectors: a one-byte document is already out of reach); the container header,
block index and writer, `write_string` and the field-name sanitizer, which build owned strings and vectors of symbolic
length (each such append costs CBMC minutes; their count bounds, grammar and identifier checks are proven separately;
the union layout is proven at concrete branch counts, the engine's bound of 255 members is out of reach); the schema
walks themselves (`root_columns`, the schema generator, the column plans), which call the engine's type constructors;
and anything else that crosses the C API into the engine (callbacks, vectors, types). Those stay with the
sqllogictests and the real-world corpus. No real bug has been found by the proofs so far.

## CI

`.github/workflows/ci.yml` has three jobs: `lint` (`cargo fmt --check`, `cargo clippy` with warnings denied, `cargo
test`), `test` (install the pinned wheel, `make`, load the extension and check `duckdb_extensions()`, `make test`) and
`kani` (the proof harnesses of `make kani` through `model-checking/kani-github-action`, without the `kani-heavy`
harnesses: 16 GiB of runner memory). Nothing of DuckDB is built; `lint` and `test` take a few minutes, `kani` about
22 (CBMC, one harness at a time).

## Performance

A single file is scanned in parallel, one Avro block per thread at a time (`init_global` allows one thread per block,
each thread claims the next block, decompresses it and decodes it straight into the output vectors with one write
per vector per chunk). `test/bigdata.avro` (1M rows of ten `long` columns, 609 deflate blocks): `count(*)` plus the
ten sums runs in 15-30 ms at 96 threads and about 205 ms single-threaded on a 96-vCPU machine, file in the page
cache. Projection pushdown skips the columns a query does not read; there is no filter pushdown.

## License

MIT, see `LICENSE`.
