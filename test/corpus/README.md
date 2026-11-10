# test/corpus: real-world Avro files

Two kinds of files live here:

- `*.avro` directly in this directory: this repository's own minimal reproductions of findings of the corpus run,
  written by `scripts/create_corpus_fixtures.py` and used by the hand-written tests in `test/sql`.
- `<source>/*.avro`: 1293 Avro object container files copied unchanged from 52 public sources (50 GitHub
  repositories, Google's `cloud-samples-data` bucket, one HuggingFace dataset), 4.6 MB in total. They are the
  test data of other Avro implementations and of systems that read or write Avro (Impala, Doris, Trino, Spark,
  Pig, Iceberg, arrow-rs, fastavro, goavro, hamba, ...), so they cover the schema features, codecs and writer
  quirks that real files have. `test/sql/corpus/<source>.test` holds the generated tests over them.

## Provenance

`MANIFEST.tsv` has one row per file: `path` (relative to this directory), `source` (the directory), `repo`,
`upstream_path` and `commit` (the file in the upstream repository, at the commit it was fetched from), `license`
(the SPDX identifier GitHub reports for the repository), `size`, `sha256`, `url` (the exact download URL) and
`duplicates` (other upstream files with the same sha256, folded into this one; none at the moment).

The files were selected from a larger corpus (1393 files found through the GitHub trees API of repositories that
have `.avro` files, plus a few public buckets) with these rules, applied by `scripts/gen_corpus_tests.py --import`:

- size at most 64 KiB (68 larger files dropped);
- a permissive license: Apache-2.0 (1045 files), MIT (239), CC-BY-4.0 (6), BSD-2-Clause (3). Dropped: GPL-3.0
  (16 files), repositories without a license (9) and with one GitHub cannot name, `NOASSERTION` (7);
- one copy per sha256 (the first in manifest order; the corpus itself was already deduplicated against the
  fixtures of `test/`).

Basenames are the upstream basenames, restricted to `[A-Za-z0-9._-]`; a name taken twice within a source gets
`-2`, `-3`, ... before the extension (`request.avro`, `request-2.avro`, `request-3.avro`).

## Licenses

`LICENSES.md` lists every source directory with its repository, commit and license, and the number of files; it is
the attribution notice for the redistributed files. Nothing in them was modified; the copyright stays with the
upstream authors.

## The generated tests

`scripts/gen_corpus_tests.py` writes one sqllogictest file per source. For every corpus file it states:

1. `SELECT count(*)` with the row count read by fastavro, an independent implementation (for 4 files fastavro
   cannot read, the count is avro-python's or, where both refuse the file, this reader's; the comment above the
   query says which);
2. `DESCRIBE` of `read_avro(...)`: the column names and types this reader produces, which locks the schema
   mapping; the generator checks the column names against the fields of the Avro root record (after the reader's
   root rules: a nullable record root unwraps, any other root is a single column);
3. the content: explicit rows for files of up to 100 rows, rendered the way DuckDB's sqllogictest runner prints
   values; for larger files the output of `SUMMARIZE` (min, max, approx_unique, avg, std, quantiles, count,
   null percentage; the avg/std/quantile strings cast to DOUBLE so that both runners compare them numerically);
   the `N values hashing to <md5>` form where a value contains a tab or newline. Before an expectation is written
   the generator compares a canonical dump of every row from this reader with the same dump from fastavro
   (`scripts/corpus_canon.py`, the comparison of the corpus run: temporal values as epoch counts, binaries as
   hex, decimals as text, unions as tag + value): a file whose dumps differ is reported and gets no content
   expectation. 1254 files are cross-checked that way; the 4 files fastavro cannot read are not.
4. a writer round trip: `COPY (...) TO '{TEST_DIR}/x.avro' (FORMAT avro)` and `EXCEPT ALL` in both directions
   expecting 0 rows. A nullable `UNION` column comes back with its members named after their branch index
   (`u0`, `u1` -> `u1`, `u2`: Avro unions are positional and the written file has `null` as the first branch), so
   for the 73 files whose schema changes that way the `DESCRIBE` of the written file is stated and the rows are
   compared on their VARCHAR rendering. Types the writer refuses (INTERVAL from `duration`, an enum whose symbol
   is not an Avro identifier) are a `statement error` with the writer's message.
5. Files the reader refuses (corrupt or truncated files, recursive schemas, a duplicate field name, a named type
   defined twice) are a `statement error` with the message; the comment says what fastavro makes of the file.

Every test file sets `threads=1`: the reader does not order a file's blocks across threads (a multi-block file
can come out in another order), and the avg/std/quantile columns of SUMMARIZE are not bitwise stable across
thread counts. The one source with a `TIMESTAMP WITH TIME ZONE` column also requires `icu` and sets
`TimeZone='UTC'`, so its rendering does not depend on the machine.

## Regenerating

```sh
make                                              # the extension the tests measure
venv/bin/pip install fastavro avro                # the oracle (or point --oracle-python at an interpreter that has them)
python3 -I scripts/gen_corpus_tests.py            # test/sql/corpus/*.test from test/corpus/MANIFEST.tsv
python3 -I scripts/gen_corpus_tests.py --import /path/to/avro-corpus/MANIFEST.tsv   # re-select the files from a corpus
```

The generator is deterministic: re-running it on the same files and build changes nothing. It exits non-zero and
prints `PROBLEM:` lines when a cross-check fails (a count or column mismatch against fastavro, differing dumps,
a round trip that is not empty), which is how a reader or writer regression shows up when the corpus is
regenerated on a new build. `--only <source>` regenerates one file.
