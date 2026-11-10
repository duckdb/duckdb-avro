#!/usr/bin/env python3
"""Benchmarks the Avro extension: one or more builds of it, each loaded into the same DuckDB CLI.

Every benchmark runs once per build through hyperfine, as `duckdb -c "LOAD '<build>'; <setup>; <query>"`, so the
timings include starting the CLI and loading the extension; the `load` benchmark measures that fixed cost alone.
The input files are generated once, by the first build, into the data directory.

Example:
    benchmark/run.py --build cpp=~/duckdb-avro-cpp/build/release/avro.duckdb_extension \\
                     --build rust=build/release/avro.duckdb_extension
"""

import argparse
import json
import os
import re
import shutil
import statistics
import subprocess
import sys
import tempfile
from pathlib import Path

from benchmarks import BENCHMARKS, DATASETS

REPO = Path(__file__).resolve().parent.parent
HIGH_VARIANCE = 0.05


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--build",
        action="append",
        default=[],
        metavar="NAME=PATH",
        help="an extension build to benchmark; the first one is the baseline the others are compared to",
    )
    parser.add_argument("--duckdb", default=shutil.which("duckdb"), help="the DuckDB CLI to load the builds into")
    parser.add_argument("--data-dir", default=str(REPO / "benchmark" / "data"), help="where the inputs are kept")
    parser.add_argument("--results-dir", default=str(REPO / "benchmark" / "results"), help="where results go")
    parser.add_argument("--filter", default="", help="only run benchmarks whose name matches this regex")
    parser.add_argument("--runs", type=int, default=10, help="timed runs per benchmark and build")
    parser.add_argument("--warmup", type=int, default=2, help="untimed runs per benchmark and build")
    parser.add_argument("--threads", type=int, default=0, help="DuckDB threads; 0 keeps DuckDB's default")
    parser.add_argument("--regenerate", action="store_true", help="regenerate the inputs")
    parser.add_argument("--force", action="store_true", help="run even if the machine is busy")
    parser.add_argument("--list", action="store_true", help="list the benchmarks and exit")
    return parser.parse_args()


def parse_builds(specs):
    builds = []
    for spec in specs:
        name, sep, path = spec.partition("=")
        if not sep or not name or not path:
            sys.exit(f"--build expects NAME=PATH, got {spec!r}")
        path = Path(os.path.expanduser(path)).resolve()
        if not path.is_file():
            sys.exit(f"extension build {path} does not exist")
        builds.append((name, path))
    if len({name for name, _ in builds}) != len(builds):
        sys.exit("build names must be unique")
    return builds


def load_average():
    return os.getloadavg()[0]


def check_machine(force):
    cores = os.cpu_count() or 1
    load = load_average()
    if load > cores * 0.25:
        message = f"1-minute load average {load:.1f} is high for {cores} cores; results would be unreliable"
        if not force:
            sys.exit(message + " (use --force to run anyway)")
        print(f"warning: {message}", file=sys.stderr)


def sql_literal(text):
    return "'" + text.replace("'", "''") + "'"


def duckdb_command(duckdb, build, statements):
    script = "; ".join([f"LOAD {sql_literal(str(build))}", *statements]) + ";"
    return [duckdb, "-unsigned", "-no-agent", "-batch", "-c", script]


def run_duckdb(duckdb, build, statements):
    command = duckdb_command(duckdb, build, statements)
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode != 0 or "Error" in result.stderr:
        sys.exit(f"failed: {' '.join(command)}\n{result.stdout}{result.stderr}")
    return result.stdout


def generate_data(duckdb, generator, data_dir, regenerate):
    data_dir.mkdir(parents=True, exist_ok=True)
    for dataset in DATASETS:
        for codec in dataset.codecs:
            path = data_dir / dataset.file_name(codec)
            if path.exists() and not regenerate:
                continue
            print(f"generating {path.name}")
            tmp = path.with_suffix(".tmp")
            run_duckdb(
                duckdb,
                generator,
                [f"COPY ({dataset.query}) TO {sql_literal(str(tmp))} (FORMAT AVRO, CODEC {sql_literal(codec)})"],
            )
            tmp.replace(path)


def hyperfine(commands, runs, warmup, export):
    command = ["hyperfine", "--shell=none", "--runs", str(runs), "--warmup", str(warmup), "--export-json", str(export)]
    for name, args in commands:
        command += ["--command-name", name, " ".join(shell_quote(arg) for arg in args)]
    subprocess.run(command, check=True)
    with open(export) as file:
        return json.load(file)["results"]


def shell_quote(arg):
    if re.fullmatch(r"[A-Za-z0-9_./=:+-]+", arg):
        return arg
    return "'" + arg.replace("'", "'\"'\"'") + "'"


def measurements(results, key, legacy_key):
    if legacy_key in results:
        return results[legacy_key] or []
    return [m[key]["value"] for m in results.get("measurements", []) if key in m]


def summarize(name, results):
    times = measurements(results, "time_wall_clock", "times")
    memory = measurements(results, "memory_peak_resident", "memory_usage_byte")
    median = statistics.median(times)
    stddev = statistics.stdev(times) if len(times) > 1 else 0.0
    return {
        "build": name,
        "median": median,
        "min": min(times),
        "max": max(times),
        "stddev": stddev,
        "noisy": statistics.mean(times) > 0 and stddev / statistics.mean(times) > HIGH_VARIANCE,
        "memory": statistics.median(memory) if memory else None,
    }


def format_seconds(seconds):
    return f"{seconds * 1000:.1f} ms" if seconds < 1 else f"{seconds:.3f} s"


def format_bytes(value):
    return "n/a" if value is None else f"{value / 2**20:.0f} MiB"


def report(rows, builds, path, metadata):
    baseline = builds[0][0]
    lines = [
        "# Avro extension benchmarks",
        "",
        *[f"- {key}: {value}" for key, value in metadata.items()],
        "",
        "Times are medians of the timed runs, with [min, max]. Ratios below 1 are faster than the baseline "
        f"(`{baseline}`); `!` marks a coefficient of variation above {HIGH_VARIANCE:.0%}.",
        "",
        "| benchmark | " + " | ".join(name for name, _ in builds) + " | " + " | ".join(
            f"{name}/{baseline}" for name, _ in builds[1:]
        ) + " |",
        "|---|" + "---|" * (2 * len(builds) - 1),
    ]
    for benchmark, summaries in rows:
        by_build = {summary["build"]: summary for summary in summaries}
        cells = [
            f"{format_seconds(s['median'])} [{format_seconds(s['min'])}, {format_seconds(s['max'])}]"
            + (" !" if s["noisy"] else "")
            for s in (by_build[name] for name, _ in builds)
        ]
        ratios = [f"{by_build[name]['median'] / by_build[baseline]['median']:.2f}" for name, _ in builds[1:]]
        lines.append(f"| {benchmark} | " + " | ".join(cells + ratios) + " |")
    lines += [
        "",
        "Peak resident memory (median):",
        "",
        "| benchmark | " + " | ".join(name for name, _ in builds) + " |",
        "|---|" + "---|" * len(builds),
    ]
    for benchmark, summaries in rows:
        by_build = {summary["build"]: summary for summary in summaries}
        cells = [format_bytes(by_build[name]["memory"]) for name, _ in builds]
        lines.append(f"| {benchmark} | " + " | ".join(cells) + " |")
    text = "\n".join(lines) + "\n"
    path.write_text(text)
    print()
    print(text)


def main():
    args = parse_args()
    if args.list:
        for benchmark in BENCHMARKS:
            print(benchmark.name)
        return
    if not args.duckdb:
        sys.exit("no DuckDB CLI found; pass --duckdb")
    if not shutil.which("hyperfine"):
        sys.exit("hyperfine is required")
    if not args.build:
        sys.exit("pass at least one --build")
    builds = parse_builds(args.build)
    data_dir = Path(args.data_dir)
    selected = [benchmark for benchmark in BENCHMARKS if re.search(args.filter, benchmark.name)]
    if not selected:
        sys.exit("no benchmark matches the filter")

    version = subprocess.run([args.duckdb, "--version"], capture_output=True, text=True).stdout.strip()
    generate_data(args.duckdb, builds[0][1], data_dir, args.regenerate)
    check_machine(args.force)
    start_load = load_average()

    results_dir = Path(args.results_dir)
    results_dir.mkdir(parents=True, exist_ok=True)
    rows = []
    with tempfile.TemporaryDirectory(prefix="avro-bench-") as scratch:
        for benchmark in selected:
            setup = [f"SET threads = {args.threads}"] if args.threads else []
            commands = []
            for name, path in builds:
                statements = benchmark.statements(str(data_dir), os.path.join(scratch, f"{name}-out"))
                commands.append((name, duckdb_command(args.duckdb, path, setup + statements)))
            print(f"\n=== {benchmark.name}")
            results = hyperfine(commands, args.runs, args.warmup, results_dir / f"{benchmark.name}.json")
            rows.append((benchmark.name, [summarize(name, result) for (name, _), result in zip(builds, results)]))

    metadata = {
        "DuckDB": version,
        "builds": ", ".join(f"`{name}` = `{path}`" for name, path in builds),
        "threads": args.threads or "default",
        "runs": f"{args.runs} (+{args.warmup} warm-up)",
        "1-minute load average before the runs": f"{start_load:.2f} on {os.cpu_count()} cores",
    }
    report(rows, builds, results_dir / "summary.md", metadata)


if __name__ == "__main__":
    main()
