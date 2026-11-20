"""The inputs and benchmarks of benchmark/run.py."""

import os
from dataclasses import dataclass, field


@dataclass(frozen=True)
class Dataset:
    name: str
    query: str
    codecs: tuple = ("null",)

    def file_name(self, codec):
        return f"{self.name}-{codec}.avro"


@dataclass(frozen=True)
class Benchmark:
    name: str
    sql: str
    dataset: Dataset = field(default=None)
    codec: str = "null"

    def statements(self, data_dir, out_prefix):
        values = {"out": f"{out_prefix}-{self.name}.avro"}
        if self.dataset:
            values["file"] = os.path.join(data_dir, self.dataset.file_name(self.codec))
            values["query"] = self.dataset.query
        return [self.sql.format(**{key: value.replace("'", "''") if key != "query" else value for key, value in values.items()})]


NUMBERS = Dataset(
    "numbers",
    "SELECT i::BIGINT AS id, (i % 1000)::INTEGER AS small, i * 0.5 AS d, (i % 7)::FLOAT AS f, i % 3 = 0 AS flag "
    "FROM range(5000000) t(i)",
    ("null", "deflate", "snappy", "zstandard"),
)
STRINGS = Dataset(
    "strings",
    "SELECT md5(i::VARCHAR) AS hash, 's' || (i % 1000)::VARCHAR AS short, repeat(chr(97 + (i % 26)::INTEGER), 100) "
    "AS long, md5(i::VARCHAR)::BLOB AS bytes FROM range(2000000) t(i)",
    ("null", "snappy"),
)
NESTED = Dataset(
    "nested",
    "SELECT [i, i + 1, i + 2, i + 3, i + 4, i + 5, i + 6, i + 7]::BIGINT[] AS items, "
    "{'a': (i % 100)::INTEGER, 'b': 'v' || (i % 10)::VARCHAR} AS record, "
    "MAP {'x': i, 'y': i * 2, 'z': i * 3} AS mapping FROM range(1000000) t(i)",
)
TEMPORAL = Dataset(
    "temporal",
    "SELECT DATE '2000-01-01' + (i % 10000)::INTEGER AS day, TIMESTAMP '2000-01-01' + to_microseconds(i) AS ts, "
    "TIMESTAMPTZ '2000-01-01 00:00:00+00' + to_microseconds(i) AS tstz, (i / 1000)::DECIMAL(18, 3) AS dec18, "
    "(i / 7)::DECIMAL(38, 10) AS dec38, md5(i::VARCHAR)::UUID AS uuid FROM range(2000000) t(i)",
)
NULLABLE = Dataset(
    "nullable",
    "SELECT CASE WHEN i % 2 = 0 THEN i END AS id, CASE WHEN i % 3 = 0 THEN 'v' || i::VARCHAR END AS text "
    "FROM range(5000000) t(i)",
)

DATASETS = [NUMBERS, STRINGS, NESTED, TEMPORAL, NULLABLE]

FULL_SCAN = "SELECT sum(hash(COLUMNS(*))) FROM read_avro('{file}')"
WRITE = "COPY ({query}) TO '{out}' (FORMAT AVRO, CODEC '{codec}')"


def read_benchmarks():
    for dataset in DATASETS:
        for codec in dataset.codecs:
            yield Benchmark(f"read_{dataset.name}_{codec}", FULL_SCAN, dataset, codec)
    yield Benchmark("read_numbers_count", "SELECT count(*) FROM read_avro('{file}')", NUMBERS)
    yield Benchmark("read_numbers_one_column", "SELECT sum(id) FROM read_avro('{file}')", NUMBERS)


def write_benchmarks():
    for dataset in DATASETS:
        for codec in dataset.codecs:
            yield Benchmark(f"write_{dataset.name}_{codec}", WRITE.replace("{codec}", codec), dataset, codec)


BENCHMARKS = [
    Benchmark("load", "SELECT 1"),
    *read_benchmarks(),
    *write_benchmarks(),
]
