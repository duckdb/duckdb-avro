# Testing this extension
This directory contains all the tests for this extension. The `sql` directory holds tests that are written as [SQLLogicTests](https://duckdb.org/dev/sqllogictest/intro.html). DuckDB aims to have most its tests in this format as SQL statements, so for this extension too new coverage goes there first.

The root makefile contains the targets that build the extension and run these tests (see its header):
```bash
make venv                                    # once: a virtualenv with the pinned duckdb wheel
make test                                    # all of test/sql through scripts/run_sqllogic.py on the pinned wheel
make unittest DUCKDB=<duckdb binary>         # the same files through DuckDB's own C++ sqllogictest runner (test/unittest next to the binary)
```
The tests name their fixtures relative to the repository root (`test/*.avro`); hand-built malformed fixtures come from `scripts/create_malformed.py`, the minimal reproductions of the real-world corpus findings under `test/corpus/` from `scripts/create_corpus_fixtures.py` (both stdlib only), the others from the `scripts/create_*.py` generators and the C++ extension's test data.

`test/corpus/<source>/` holds 1293 real-world Avro files copied from public test suites (provenance and licenses in
`test/corpus/MANIFEST.tsv`, `LICENSES.md` and `README.md` there); `scripts/gen_corpus_tests.py` generates
`test/sql/corpus/<source>.test` over them (row counts from fastavro, schema and content from this reader after a
row-by-row cross-check against fastavro, a writer round trip, the error for refused files). Those files are
generated: do not edit them, re-run the generator (`test/corpus/README.md`).
