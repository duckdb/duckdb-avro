# Writer cleanup tests

Enable the fault-injection tests when configuring DuckDB with this extension:

```sh
make debug EXT_FLAGS="-DAVRO_BUILD_WRITER_TESTS=ON"
./build/debug/extension/avro/avro_writer_tests
```

The tests inject filesystem write errors and Avro allocation failures during
`COPY`, exercise header-buffer retries, and discard pending blocks before and
after failed flushes. They count outstanding Avro allocations after every
operation. Run a sanitizer build on Linux to also check allocations made directly
by compression libraries.

The Linux SQL test `test/sql/copy/write_failure.test` exercises a constructor
write failure in the regular SQL test suite.
