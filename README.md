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
| `avro_metadata(path)` | the key-value entries of a file's header (`avro.schema`, `avro.codec`, anything the writer added) |
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
| `bytes` or `fixed` + `decimal` | `DECIMAL(precision, scale)`; `BLOB` when the attributes are invalid (no integer precision, precision outside 1..38, scale outside 0..precision) |
| `enum` | `ENUM` of the symbols |
| `record` (or `error`, the protocol error record) | `STRUCT`; a record without fields is `"NULL"` |
| `array` | `LIST`; with `"logicalType": "map"` over key/value records a `MAP(key, value)` |
| `map` | `MAP(VARCHAR, value)` |
| `union` | null branches dropped; one branch left: that type, nullable; none left: `"NULL"`; several: `UNION(u<i> T, ...)`, `i` the branch index |

Logical types are honoured on primitives only; an array's `map` annotation is the one exception. An unknown logical
type (`char`, `time-interval-micros`, any vendor name) or an invalid one is ignored and the underlying type is read,
as the specification asks. Recursive records are not supported (DuckDB has no recursive types).

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

Every field, element, key and value is written as `["null", T]` unless `FIELD_IDS` marks it `__duckdb_nullable:
false`. Other types (`TINYINT` ... `HUGEINT`, unsigned integers, `ENUM`, `UNION`, `INTERVAL`, `ARRAY`, ...) are
refused with `Can't convert logical type ... to Avro type`.

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
- `test/`: the upstream suite, `test/sql/*.test` and `test/sql/copy/*.test`, with the fixture files.
- `scripts/`: `pin_api_table.py` (re-pinning), `append_extension_metadata.py` (the footer), `run_sqllogic.py` (the
  test runner), and the generators of the independent fixtures (`create_decimal_bytes.py`, `create_timestamp_*.py`,
  `create_malformed.py`: the hand-built malformed files, `create_corpus_fixtures.py`: the `test/corpus` reproductions of
  the real-world corpus findings; both stdlib only).

## Tests

`make test` runs all 34 test files (`test/sql/*.test`, `test/sql/copy/*.test` and `*.test_slow`) with
`scripts/run_sqllogic.py`, a sqllogictest runner on the pinned Python wheel that follows DuckDB's C++ runner for the
directives the tests use. `make unittest DUCKDB=<binary>` runs the same files through the C++ runner (above);
`TESTS=` selects a subset. `cargo test --release` runs the unit tests of the crates (codec round trips, schema JSON,
the decoder).

## CI

`.github/workflows/ci.yml` has two jobs: `lint` (`cargo fmt --check`, `cargo clippy` with warnings denied, `cargo
test`) and `test` (install the pinned wheel, `make`, load the extension and check `duckdb_extensions()`, `make test`).
Nothing of DuckDB is built; a run takes a few minutes.

## Performance

A single file is scanned in parallel, one Avro block per thread at a time (`init_global` allows one thread per block,
each thread claims the next block, decompresses it and decodes it straight into the output vectors with one write
per vector per chunk). `test/bigdata.avro` (1M rows of ten `long` columns, 609 deflate blocks): `count(*)` plus the
ten sums runs in 15-30 ms at 96 threads and about 205 ms single-threaded on a 96-vCPU machine, file in the page
cache. Projection pushdown skips the columns a query does not read; there is no filter pushdown.

## License

MIT, see `LICENSE`.
