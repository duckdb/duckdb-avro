#!/usr/bin/env python3
"""Cross-checks the avro-core dump tool against fastavro.

usage: python3 -I oracle_check.py DUMP_BINARY FILE...

For every file: runs `DUMP_BINARY FILE`, reads the same file with fastavro (logical-type readers disabled, so the
raw underlying values are compared, as the C++ extension does) and compares the codec, the metadata, the block
row counts and every row in the normalized JSON shape the dump tool prints (unions as the branch value, bytes and
fixed as lowercase hex, enums as symbols, floats widened, NaN/Infinity as strings).
"""

import io
import json
import math
import subprocess
import sys

import fastavro
import fastavro.read

# raw values: no datetime / Decimal / UUID conversion
fastavro.read.LOGICAL_READERS.clear()


def normalize(value):
    if isinstance(value, bytes):
        return value.hex()
    if isinstance(value, bool) or value is None or isinstance(value, (int, str)):
        return value
    if isinstance(value, float):
        if math.isnan(value):
            return "NaN"
        if math.isinf(value):
            return "Infinity" if value > 0 else "-Infinity"
        return value
    if isinstance(value, dict):
        return {k: normalize(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [normalize(v) for v in value]
    raise TypeError(f"unexpected oracle value type {type(value)}: {value!r}")


def run_dump(binary, path):
    proc = subprocess.run([binary, path], capture_output=True, text=True)
    if proc.returncode != 0:
        return None, proc.stderr.strip()
    lines = proc.stdout.split("\n")
    header = {}
    rows = []
    in_rows = False
    for line in lines:
        if in_rows:
            if line:
                rows.append(json.loads(line))
        elif line == "--- rows":
            in_rows = True
        elif line:
            key, _, rest = line.partition(": ")
            header[key] = rest
    return (header, rows), None


def oracle(path):
    with open(path, "rb") as f:
        data = f.read()
    reader = fastavro.reader(io.BytesIO(data))
    rows = [normalize(r) for r in reader]
    meta = dict(reader.metadata)
    codec = reader.codec
    blocks = [b.num_records for b in fastavro.block_reader(io.BytesIO(data))]
    return meta, codec, blocks, rows


def check(binary, path):
    dumped, err = run_dump(binary, path)
    if err is not None:
        return f"DUMP ERROR: {err}"
    header, rows = dumped
    try:
        meta, codec, blocks, oracle_rows = oracle(path)
    except Exception as e:  # noqa: BLE001
        return f"ORACLE ERROR: {e!r}"
    problems = []
    if header.get("codec") != codec:
        problems.append(f"codec {header.get('codec')!r} != {codec!r}")
    dumped_meta = json.loads(header.get("metadata", "{}"))
    if dumped_meta != meta:
        problems.append(f"metadata differs: {sorted(set(dumped_meta) ^ set(meta))} / values {[k for k in meta if dumped_meta.get(k) != meta[k]]}")
    # "blocks: N rows: M per_block: [a,b,...]"
    blocks_line = header.get("blocks", "")
    per_block = blocks_line.partition("per_block: ")[2].strip("[]")
    dumped_blocks = [int(x) for x in per_block.split(",") if x]
    if dumped_blocks != blocks:
        problems.append(f"blocks {dumped_blocks} != {blocks}")
    if len(rows) != len(oracle_rows):
        problems.append(f"row count {len(rows)} != {len(oracle_rows)}")
    else:
        for i, (a, b) in enumerate(zip(rows, oracle_rows)):
            if a != b:
                problems.append(f"row {i} differs:\n    dump   {json.dumps(a)[:400]}\n    oracle {json.dumps(b)[:400]}")
                if len(problems) > 5:
                    break
    if problems:
        return "MISMATCH: " + "; ".join(problems)
    return f"OK ({len(rows)} rows, {len(blocks)} blocks, codec {codec})"


def main():
    binary, files = sys.argv[1], sys.argv[2:]
    bad = 0
    for path in files:
        result = check(binary, path)
        bad += not result.startswith("OK")
        print(f"{path}: {result}")
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    main()
