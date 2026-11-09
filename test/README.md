# Testing this extension
This directory contains all the tests for this extension. The `sql` directory holds tests that are written as [SQLLogicTests](https://duckdb.org/dev/sqllogictest/intro.html). DuckDB aims to have most its tests in this format as SQL statements, so for this extension too new coverage goes there first.

The root makefile contains the targets that build the extension and run these tests (see its header):
```bash
make venv                                    # once: a virtualenv with the pinned duckdb wheel
make test                                    # all of test/sql through scripts/run_sqllogic.py on the pinned wheel
make unittest DUCKDB=<duckdb binary>         # the same files through DuckDB's own C++ sqllogictest runner (test/unittest next to the binary)
```
The tests name their fixtures relative to the repository root (`test/*.avro`); hand-built malformed fixtures come from `scripts/create_malformed.py`, the others from the `scripts/create_*.py` generators and the C++ extension's test data.
