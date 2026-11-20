# Benchmarks

`benchmark/run.py` times builds of the extension against each other with [hyperfine](https://github.com/sharkdp/hyperfine).
Every build is loaded into the same DuckDB CLI, which must be the DuckDB version the builds target (`v2.0.0`), so the
builds differ in nothing but the extension. The first build is the baseline the others are compared to.

```shell
# the C++ implementation the Rust rewrite replaced, as the baseline
benchmark/build_baseline.sh
# the Rust implementation
make release

benchmark/run.py \
    --build cpp=$HOME/duckdb-avro-cpp/build/release/avro.duckdb_extension \
    --build rust=build/release/avro.duckdb_extension
```

- The inputs are generated once, with the baseline, into `benchmark/data` (`--regenerate` to redo it). They cover
  numbers under every codec, strings and blobs, nested types, temporal and decimal types, and mostly-NULL columns.
- `read_*` benchmarks scan a file in full (`sum(hash(COLUMNS(*)))`), or count it or read one column; `write_*`
  benchmarks `COPY` the query behind an input to a new file. `load` is the fixed cost of starting the CLI and loading
  the extension, which every other timing includes.
- `--filter <regex>` selects benchmarks, `--threads <n>` pins the thread count, `--runs`/`--warmup` set the repetitions.
- Results go to `benchmark/results`: one hyperfine JSON per benchmark and `summary.md` with the median, min and max per
  build, the ratio to the baseline, and the load average before the runs (the runs themselves load the machine). Runs whose coefficient of variation exceeds
  5% are marked; differences within that noise are not meaningful.
- The script refuses to start when the 1-minute load average exceeds a quarter of the cores (`--force` overrides).
  Run nothing else while benchmarking, and benchmark release builds only.
