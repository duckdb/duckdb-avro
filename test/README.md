# Testing this extension
This directory contains all the tests for this extension. The `sql` directory holds tests that are written as [SQLLogicTests](https://duckdb.org/dev/sqllogictest/intro.html). DuckDB aims to have most its tests in this format as SQL statements, so for the quack extension, this should probably be the goal too.

The root makefile contains targets to build and run all of these tests. To run the SQLLogicTests:
```bash
make test
```
or 
```bash
make test_debug
```

The tests name their fixtures relative to the repository root (`test/*.avro`). The hand-built malformed fixtures come from `scripts/create_malformed.py` and the minimal reproductions under `test/corpus/` from `scripts/create_corpus_fixtures.py` (both standard library only). `test/corpus/<source>/` holds 1293 real-world Avro files from public test suites, read by `test/sql/corpus/<source>.test`; provenance, licenses and how those tests were made are in `test/corpus/README.md`, `MANIFEST.tsv` and `LICENSES.md`.
