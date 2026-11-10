#!/usr/bin/env python3 -I
"""Generates test/sql/corpus/<source>.test from the real-world Avro files under test/corpus (standard library only).

    python3 -I scripts/gen_corpus_tests.py [--jobs N] [--only SOURCE] [--python venv/bin/python]
                                           [--oracle-python PATH] [--extension build/avro.duckdb_extension]
    python3 -I scripts/gen_corpus_tests.py --import /path/to/avro-corpus/MANIFEST.tsv

`--import` selects the files of a corpus (see test/corpus/README.md: size <= 64 KiB, a permissive license, one copy
per sha256), copies them to test/corpus/<source>/<basename> and writes test/corpus/MANIFEST.tsv and
test/corpus/LICENSES.md. Without `--import` the script reads test/corpus/MANIFEST.tsv and writes one test file per
source. The output is deterministic: re-running it changes nothing.

Two interpreters do the measuring, each as a subprocess of this script running the same file in a worker mode:
`--python` (default venv/bin/python, the pinned DuckDB wheel of `make venv`) runs our extension, `--oracle-python`
(default $AVRO_ORACLE_PYTHON, else venv/bin/python; needs the `fastavro` and `avro` packages: `venv/bin/pip install
fastavro avro`) runs the independent oracle. The test files take all expectations that an oracle can give from it:

- the row count is fastavro's (avro-python's where fastavro refuses a file; ours, flagged in a comment, where both do);
- the schema (`DESCRIBE`) is ours, with the top-level column names checked against the fields of the Avro root
  record (after the reader's root rules: a nullable-record root unwraps, any other root is one column);
- the content is ours, rendered as DuckDB's sqllogictest runner renders values (scripts/run_sqllogic.py), and only
  emitted when a row-by-row comparison of the canonical dumps of both readers (scripts/corpus_canon.py, the method
  of the corpus run) agrees: explicit rows for files up to 100 rows, `SUMMARIZE` for larger ones (its avg/std/
  quantiles cast to DOUBLE so the runners compare them numerically), the `N values hashing to <md5>` form where a
  value cannot be written on a result line (tabs, newlines) or SUMMARIZE fails;
- a writer round trip (`COPY ... (FORMAT avro)` to the test directory, `EXCEPT ALL` both ways) for every readable
  file; where the written file reads back with other types (an Avro union is positional: a nullable UNION column
  comes back with members named after their branch index) the comparison is on the VARCHAR rendering; a type the
  writer refuses is a `statement error` with its message;
- a file the reader refuses (corrupt, truncated, a recursive schema) is a `statement error` with the message.

Every file sets `threads=1`: the reader does not order the blocks of a file across threads, and SUMMARIZE's
avg/std/quantiles are not bitwise stable across thread counts.
"""
import argparse
import csv
import hashlib
import json
import math
import os
import re
import shutil
import subprocess
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
CORPUS = os.path.join(ROOT, "test", "corpus")
MANIFEST = os.path.join(CORPUS, "MANIFEST.tsv")
LICENSES = os.path.join(CORPUS, "LICENSES.md")
TESTS = os.path.join(ROOT, "test", "sql", "corpus")

MAX_SIZE = 64 * 1024
# SPDX ids of the licenses whose files are copied; anything else (GPL, "NONE", "NOASSERTION") is dropped
PERMISSIVE = {
    "Apache-2.0", "MIT", "BSD-2-Clause", "BSD-3-Clause", "BSD-3-Clause-Clear", "0BSD", "ISC", "CC-BY-4.0", "CC0-1.0",
    "Unlicense", "Zlib", "MPL-2.0", "PSF-2.0", "Python-2.0", "BSL-1.0", "Artistic-2.0",
}
EXPLICIT_ROWS_MAX = 100
MANIFEST_COLUMNS = ["path", "source", "repo", "upstream_path", "commit", "license", "size", "sha256", "url",
                    "duplicates"]
SUMMARIZE_COLUMNS = ("column_name, column_type, min, max, approx_unique, TRY_CAST(avg AS DOUBLE) AS avg, "
                     "TRY_CAST(std AS DOUBLE) AS std, TRY_CAST(q25 AS DOUBLE) AS q25, TRY_CAST(q50 AS DOUBLE) AS q50, "
                     "TRY_CAST(q75 AS DOUBLE) AS q75, count, null_percentage")
SUMMARIZE_TYPES = "TTTTIRRRRRIR"


def sql_str(s):
    return "'" + s.replace("'", "''") + "'"


def read_manifest(path):
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f, delimiter="\t"))


# -------------------------------------------------------------------------------------------------
# --import: selection and copy
# -------------------------------------------------------------------------------------------------


def sanitize(name):
    return re.sub(r"[^A-Za-z0-9._-]", "_", name)


def import_corpus(source_manifest):
    rows = read_manifest(source_manifest)
    corpus_root = os.path.dirname(os.path.abspath(source_manifest))
    dropped = {}
    by_sha = {}
    selected = []
    for r in rows:
        if int(r["size"]) > MAX_SIZE:
            dropped["size"] = dropped.get("size", 0) + 1
            continue
        if r["license"] not in PERMISSIVE:
            dropped[r["license"]] = dropped.get(r["license"], 0) + 1
            continue
        first = by_sha.get(r["sha256"])
        if first is not None:
            first["duplicates"].append(f"{r['repo']}:{r['path']}")
            continue
        r = dict(r, duplicates=[])
        by_sha[r["sha256"]] = r
        selected.append(r)
    # one directory per source, basenames sanitized, collisions numbered in manifest order
    taken = set()
    out = []
    for r in selected:
        source = sanitize(r["source"])
        base = sanitize(os.path.basename(r["path"])) or "file"
        stem, ext = os.path.splitext(base)
        name, n = base, 1
        while (source, name) in taken:
            n += 1
            name = f"{stem}-{n}{ext}"
        taken.add((source, name))
        rel = f"{source}/{name}"
        out.append({
            "path": rel, "source": r["source"], "repo": r["repo"], "upstream_path": r["path"], "commit": r["commit"],
            "license": r["license"], "size": r["size"], "sha256": r["sha256"], "url": r["url"],
            "duplicates": " ".join(r["duplicates"]), "_src": os.path.join(corpus_root, r["local_path"]),
        })
    out.sort(key=lambda r: (r["source"], r["path"]))
    # replace the per-source directories
    for d in sorted({r["path"].split("/")[0] for r in out}):
        full = os.path.join(CORPUS, d)
        if os.path.isdir(full):
            shutil.rmtree(full)
        os.makedirs(full)
    for r in out:
        shutil.copyfile(r["_src"], os.path.join(CORPUS, r["path"]))
        with open(os.path.join(CORPUS, r["path"]), "rb") as f:
            digest = hashlib.sha256(f.read()).hexdigest()
        if digest != r["sha256"]:
            raise SystemExit(f"{r['_src']}: sha256 {digest} does not match the manifest")
    with open(MANIFEST, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, MANIFEST_COLUMNS, delimiter="\t", extrasaction="ignore", lineterminator="\n")
        w.writeheader()
        w.writerows(out)
    write_licenses(out)
    kept = {}
    for r in out:
        kept[r["license"]] = kept.get(r["license"], 0) + 1
    print(f"{len(out)} files copied to {CORPUS} ({sum(int(r['size']) for r in out)} bytes)")
    print("kept by license:", ", ".join(f"{k} {v}" for k, v in sorted(kept.items())))
    print("dropped:", ", ".join(f"{k} {v}" for k, v in sorted(dropped.items())))
    print("duplicates folded:", sum(len(r["duplicates"].split()) for r in out if r["duplicates"]))


def write_licenses(rows):
    by_repo = {}
    for r in rows:
        e = by_repo.setdefault(r["repo"], {"license": r["license"], "commit": r["commit"], "n": 0, "size": 0,
                                           "source": r["source"], "url": r["url"]})
        e["n"] += 1
        e["size"] += int(r["size"])
        if e["license"] != r["license"]:
            e["license"] += ", " + r["license"]
    lines = [
        "# Licenses of the files under test/corpus",
        "",
        "The Avro files in the per-source directories are test data copied unchanged from the repositories below",
        "(the exact upstream path, commit and sha256 of every file is in `MANIFEST.tsv`; `test/corpus/*.avro` without",
        "a directory are this repository's own fixtures from `scripts/create_corpus_fixtures.py`). They are",
        "redistributed here under their repository's license, named by its SPDX identifier as GitHub reports it; the",
        "copyright stays with the respective authors. Nothing was modified. Where a license asks for the notice to be",
        "kept, the repository link below is that notice: the full license text is the `LICENSE` file of the",
        "repository at the given commit.",
        "",
        "| directory | repository | license | files | bytes |",
        "|---|---|---|---|---|",
    ]
    for repo in sorted(by_repo, key=lambda k: by_repo[k]["source"]):
        e = by_repo[repo]
        # GitHub repositories link to the tree at the commit, other hosts to the directory of their first file
        url = (f"https://github.com/{repo}/tree/{e['commit']}" if e["source"].startswith("gh_")
               else e["url"].rsplit("/", 1)[0] + "/")
        lines.append(f"| `{sanitize(e['source'])}/` | [{repo}]({url}) | {e['license']} | {e['n']} | {e['size']} |")
    licenses = {}
    for e in by_repo.values():
        for lic in e["license"].split(", "):
            licenses[lic] = licenses.get(lic, 0) + e["n"]
    lines += ["", "Files per license: " + ", ".join(f"{k} {v}" for k, v in sorted(licenses.items())) + ".", ""]
    with open(LICENSES, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


# -------------------------------------------------------------------------------------------------
# the SQL of the tests (shared by the worker that measures and the writer that emits)
# -------------------------------------------------------------------------------------------------


def read_sql(rel):
    return f"read_avro({sql_str('test/corpus/' + rel)})"


def q_count(rel):
    return f"SELECT count(*) FROM {read_sql(rel)}"


def q_describe(rel):
    return f"SELECT column_name, column_type FROM (DESCRIBE SELECT * FROM {read_sql(rel)})"


def q_rows(rel):
    return f"SELECT * FROM {read_sql(rel)}"


def q_summarize(rel):
    return f"SELECT {SUMMARIZE_COLUMNS} FROM (SUMMARIZE SELECT * FROM {read_sql(rel)})"


def rt_path(rel):
    return "{TEST_DIR}/" + rel.split("/")[-1]


def q_copy(rel):
    return f"COPY (SELECT * FROM {read_sql(rel)}) TO {sql_str(rt_path(rel))} (FORMAT avro)"


def q_except(rel, written_first, as_text):
    cols = "COLUMNS(*)::VARCHAR" if as_text else "*"
    a = f"SELECT {cols} FROM read_avro({sql_str(rt_path(rel))})"
    b = f"SELECT {cols} FROM {read_sql(rel)}"
    left, right = (a, b) if written_first else (b, a)
    return f"SELECT count(*) FROM ({left} EXCEPT ALL {right})"


def q_describe_written(rel):
    return f"SELECT column_name, column_type FROM (DESCRIBE SELECT * FROM read_avro({sql_str(rt_path(rel))}))"


# -------------------------------------------------------------------------------------------------
# --duck-worker: our extension on the pinned wheel
# -------------------------------------------------------------------------------------------------


def duck_worker(extension, in_path, out_path):
    sys.path.insert(0, HERE)
    import duckdb  # noqa: PLC0415
    import corpus_canon as canon  # noqa: PLC0415
    from run_sqllogic import hash_rendered, render_rows  # noqa: PLC0415

    with open(in_path, encoding="utf-8") as f:
        job = json.load(f)
    test_dir = tempfile.mkdtemp(prefix="corpus_gen_")
    results = {}
    for rel in job["files"]:
        con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
        con.execute(f"LOAD {sql_str(extension)}")
        con.execute("SET threads=1")
        if job.get("timezone"):
            con.execute(f"SET TimeZone={sql_str(job['timezone'])}")
        results[rel] = measure_file(con, rel, job, test_dir, duckdb, canon, render_rows, hash_rendered)
        con.close()
    shutil.rmtree(test_dir, ignore_errors=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(results, f)


def measure_file(con, rel, job, test_dir, duckdb, canon, render_rows, hash_rendered):
    """Everything the test of one file states, measured on our reader and writer."""
    res = {}

    def run(sql):
        return con.execute(sql).fetchall()

    def error_of(fn):
        try:
            fn()
            return None
        except duckdb.Error as e:  # noqa: PERF203
            return str(e)

    try:
        res["schema_json"] = next(v for k, v in run(f"SELECT key, value FROM avro_metadata({sql_str('test/corpus/' + rel)})")
                                  if k == "avro.schema")
    except (duckdb.Error, StopIteration) as e:
        res["metadata_error"] = str(e)
    error = error_of(lambda: res.__setitem__("describe", [list(r) for r in run(q_describe(rel))]))
    if error is None:
        error = error_of(lambda: res.__setitem__("count", run(q_count(rel))[0][0]))
    if error is None:
        # a full scan, decoded and rendered the way the runner prints values
        error = error_of(lambda: res.__setitem__("rendered", render_rows(con.sql(q_rows(rel)))[0]))
    if error is not None:
        res["read_error"] = error
        return res
    res["types"] = [t.id for t in con.sql(q_rows(rel)).types]
    res["hash"] = hash_rendered([v for row in res["rendered"] for v in row])
    if len(res["rendered"]) > EXPLICIT_ROWS_MAX:
        try:
            res["summarize"] = render_rows(con.sql(q_summarize(rel)))[0]
        except duckdb.Error as e:
            res["summarize_error"] = str(e)
    # the canonical dump the oracle compares against, one JSON object per row in file order
    if "schema_json" in res:
        try:
            root, recursion = canon.parse(json.loads(res["schema_json"]))
            cols, node = canon.root_columns(root)
            if cols is None:
                name = res["describe"][0][0]
                sel = canon.duck_canon_sql(node, canon.q(name)) + " AS " + canon.q(name)
            else:
                sel = ", ".join(f"{canon.duck_canon_sql(n, canon.q(f))} AS {canon.q(f)}" for f, n in cols)
            dump = os.path.join(job["workdir"], hashlib.sha1(rel.encode()).hexdigest() + ".jsonl")
            con.execute(f"COPY (SELECT {sel} FROM {read_sql(rel)}) TO {sql_str(dump)} (FORMAT json)")
            res["canon_dump"] = dump
        except (duckdb.Error, canon.RecursiveSchema, ValueError, KeyError) as e:
            res["canon_error"] = f"{type(e).__name__}: {e}"
    # the writer round trip, in a scratch directory standing in for {TEST_DIR}
    written = os.path.join(test_dir, rel.split("/")[-1])
    if os.path.exists(written):
        os.unlink(written)
    copy_sql = q_copy(rel).replace("{TEST_DIR}", test_dir)
    rt = {}
    res["roundtrip"] = rt
    error = error_of(lambda: run(copy_sql))
    if error is not None:
        rt["error"] = error
        return res
    rt["describe"] = [list(r) for r in run(q_describe_written(rel).replace("{TEST_DIR}", test_dir))]
    as_text = rt["describe"] != res["describe"]
    rt["as_text"] = as_text
    for key, written_first in (("written_minus_original", True), ("original_minus_written", False)):
        sql = q_except(rel, written_first, as_text).replace("{TEST_DIR}", test_dir)
        error = error_of(lambda: rt.__setitem__(key, run(sql)[0][0]))
        if error is not None:
            rt["compare_error"] = error
            break
    return res


# -------------------------------------------------------------------------------------------------
# --oracle-worker: fastavro (and avro-python as the second opinion), comparing the canonical dumps
# -------------------------------------------------------------------------------------------------


def oracle_worker(in_path, out_path):
    sys.path.insert(0, HERE)
    import corpus_canon as canon  # noqa: PLC0415
    import fastavro  # noqa: PLC0415
    import fastavro.read  # noqa: PLC0415

    fastavro.read.LOGICAL_READERS.clear()
    with open(in_path, encoding="utf-8") as f:
        job = json.load(f)
    results = {"fastavro_version": fastavro.__version__}

    def canon_equal(ours, oracle):
        """Our canonical value (parsed from the JSON dump) against the oracle's; a union whose value fits several
        branches (an int in ["int", "long"]) accepts any of them."""
        if isinstance(oracle, dict) and "_alt" in oracle:
            return (isinstance(ours, dict) and set(ours) == {"t", "v"}
                    and (ours["t"] == oracle["t"] or ours["t"] in oracle["_alt"]) and canon_equal(ours["v"], oracle["v"]))
        if isinstance(oracle, dict):
            return isinstance(ours, dict) and ours.keys() == oracle.keys() and all(
                canon_equal(ours[k], oracle[k]) for k in oracle)
        if isinstance(oracle, list):
            return isinstance(ours, list) and len(ours) == len(oracle) and all(map(canon_equal, ours, oracle))
        if isinstance(oracle, bool) or isinstance(ours, bool):
            return ours is oracle
        if isinstance(oracle, float):
            return isinstance(ours, (int, float)) and (ours == oracle or (math.isnan(ours) and math.isnan(oracle)))
        return ours == oracle

    def strip_alt(v):
        if isinstance(v, dict):
            return {k: strip_alt(x) for k, x in v.items() if k != "_alt"}
        if isinstance(v, list):
            return [strip_alt(x) for x in v]
        return v

    def canon_dump(v):
        return json.dumps(strip_alt(v), sort_keys=True, ensure_ascii=False, separators=(",", ":"))

    def avro_python_count(path):
        try:
            import avro.datafile  # noqa: PLC0415
            import avro.io  # noqa: PLC0415
            with open(path, "rb") as f:
                reader = avro.datafile.DataFileReader(f, avro.io.DatumReader())
                n = sum(1 for _ in reader)
                reader.close()
            return {"count": n}
        except Exception as e:  # noqa: BLE001
            return {"error": f"{type(e).__name__}: {e}"[:300]}

    for rel, dump in job["files"].items():
        path = os.path.join(CORPUS, rel)
        res = {}
        results[rel] = res
        try:
            with open(path, "rb") as f:
                reader = fastavro.reader(f, return_record_name=True, return_named_type=True)
                res["codec"] = reader.codec
                schema_json = reader.metadata["avro.schema"]
                root, recursion = canon.parse(json.loads(schema_json))
                res["recursive"] = sorted(set(recursion))
                cols, node = canon.root_columns(root)
                res["columns"] = None if cols is None else [f for f, _ in cols]
                dump_f = open(dump, encoding="utf-8") if dump and os.path.exists(dump) else None
                n = 0
                mismatches = 0
                example = None
                dump_rows = 0
                for rec in reader:
                    n += 1
                    if dump_f is None:
                        continue
                    if cols is None:
                        row = {None: canon.oracle_canon(node, rec)}
                    else:
                        _, inner = canon._unwrap_named(rec) if rec is not None else (None, None)
                        row = ({f: None for f, _ in cols} if inner is None
                               else {f: canon.oracle_canon(nd, inner.get(f)) for f, nd in cols})
                    line = dump_f.readline()
                    if not line:
                        continue
                    dump_rows += 1
                    ours = json.loads(line)
                    if cols is None:
                        ours = {None: next(iter(ours.values()))}
                    if not canon_equal(ours, row):
                        mismatches += 1
                        if example is None:
                            example = {"row": n - 1, "ours": canon_dump(ours)[:300], "oracle": canon_dump(row)[:300]}
                res["count"] = n
                if dump_f is not None:
                    dump_rows += sum(1 for _ in dump_f)
                    dump_f.close()
                    res["compare"] = {"dump_rows": dump_rows, "mismatches": mismatches, "example": example}
        except canon.RecursiveSchema as e:
            res["error"] = f"recursive schema: {e}"
            res["count"] = None
        except Exception as e:  # noqa: BLE001
            res["error"] = f"{type(e).__name__}: {e}"[:300]
            res["avro_python"] = avro_python_count(path)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(results, f)


# -------------------------------------------------------------------------------------------------
# generation
# -------------------------------------------------------------------------------------------------


def run_worker(python, args, in_data, workdir, tag):
    in_path = os.path.join(workdir, tag + ".in.json")
    out_path = os.path.join(workdir, tag + ".out.json")
    with open(in_path, "w", encoding="utf-8") as f:
        json.dump(in_data, f)
    cmd = [python, "-I", os.path.abspath(__file__)] + args + [in_path, out_path]
    proc = subprocess.run(cmd, capture_output=True, text=True, cwd=ROOT)
    if proc.returncode != 0:
        raise SystemExit(f"{' '.join(cmd)} failed:\n{proc.stderr[-4000:]}")
    with open(out_path, encoding="utf-8") as f:
        return json.load(f)


def error_substring(message):
    """The part of an error message a `statement error` expects: without the exception prefix, the block/offset
    prefix (which block fails first is up to the scan) and the detail in parentheses."""
    text = message.split("\n")[0]
    text = re.sub(r"^[A-Za-z ]+ Error: ", "", text)
    text = re.sub(r"^Avro block \d+ at offset \d+: ", "", text)
    text = re.sub(r"^Cannot (parse|read) file header: ", "", text)
    return text.split(" (")[0].strip()


def type_letter(type_id):
    if type_id in ("tinyint", "smallint", "integer", "bigint", "hugeint", "utinyint", "usmallint", "uinteger",
                   "ubigint", "uhugeint"):
        return "I"
    if type_id in ("float", "double", "decimal"):
        return "R"
    return "T"


def renderable(rows):
    return all("\t" not in v and "\n" not in v for row in rows for v in row)


class Report:
    def __init__(self):
        self.files = 0
        self.refused = 0
        self.explicit = 0
        self.summarized = 0
        self.hashed = 0
        self.cross_checked = 0
        self.no_oracle = []
        self.roundtrip_ok = 0
        self.roundtrip_text = 0
        self.roundtrip_refused = 0
        self.problems = []


def emit_file(source, rows, duck, oracle, report):
    """The text of test/sql/corpus/<source>.test."""
    repo = rows[0]["repo"]
    licenses = sorted({r["license"] for r in rows})
    needs_icu = any("WITH TIME ZONE" in t for r in rows
                    for _, t in (duck[r["path"]].get("describe") or []))
    out = [
        f"# name: test/sql/corpus/{source}.test",
        f"# description: {len(rows)} real-world Avro file{'s' if len(rows) != 1 else ''} from {repo} "
        f"({', '.join(licenses)})",
        "# group: [corpus]",
        "",
        "# generated by scripts/gen_corpus_tests.py - do not edit. The files are copies of upstream test data",
        "# (provenance: test/corpus/MANIFEST.tsv, licenses: test/corpus/LICENSES.md). Row counts come from fastavro,",
        f"# the content of every file was cross-checked row by row against fastavro {oracle['fastavro_version']}",
        "# before its expectation (rendered by this reader) was written down. threads=1: block order and the",
        "# SUMMARIZE aggregates are not stable across threads.",
        "",
        "require avro",
    ]
    if needs_icu:
        out += ["", "require icu"]
    out += ["", "statement ok", "SET threads=1"]
    if needs_icu:
        out += ["", "statement ok", "SET TimeZone='UTC'"]
    for r in rows:
        rel = r["path"]
        d = duck[rel]
        o = oracle.get(rel, {})
        report.files += 1
        out += ["", "", f"# ---- {rel}: {r['upstream_path']} ({r['repo']} @ {r['commit'][:10]}, {r['license']})"]
        if "read_error" in d:
            report.refused += 1
            reason = o.get("error") or (f"fastavro reads {o['count']} rows" if o.get("count") is not None else "")
            out += [f"# refused by the reader; fastavro: {reason}" if reason else "# refused by the reader",
                    "statement error", q_count(rel), "----", error_substring(d["read_error"])]
            continue
        # the row count: fastavro, avro-python, else ours (flagged)
        count = d["count"]
        if o.get("count") is not None and "error" not in o:
            if o["count"] != count:
                report.problems.append(f"{rel}: our count {count}, fastavro {o['count']}")
            note = f"# {o['count']} row{'s' if o['count'] != 1 else ''}, codec {o.get('codec')}"
            expected_count = o["count"]
        else:
            ap = o.get("avro_python") or {}
            if ap.get("count") is not None:
                expected_count = ap["count"]
                note = (f"# {expected_count} row{'s' if expected_count != 1 else ''} per avro-python (fastavro cannot "
                        f"read the file: {o.get('error')}); "
                        f"content not cross-checked")
                if expected_count != count:
                    report.problems.append(f"{rel}: our count {count}, avro-python {expected_count}")
            else:
                expected_count = count
                note = (f"# {count} row{'s' if count != 1 else ''} per this reader: no independent oracle "
                        f"(fastavro: {o.get('error')}; "
                        f"avro-python: {ap.get('error')})")
            report.no_oracle.append(f"{rel}: {o.get('error')}")
        # the schema: column names against the Avro root
        if o.get("columns") is not None:
            ours = [name for name, _ in d["describe"]]
            if ours != o["columns"]:
                report.problems.append(f"{rel}: columns {ours} differ from the Avro root fields {o['columns']}")
        elif "columns" in o and len(d["describe"]) != 1:
            report.problems.append(f"{rel}: a non-record root should read as one column, got {len(d['describe'])}")
        # the content: cross-checked?
        cmp = o.get("compare")
        checked = cmp is not None and cmp["mismatches"] == 0 and cmp["dump_rows"] == count == o.get("count")
        if cmp is not None and not checked:
            report.problems.append(f"{rel}: canonical dumps differ: {cmp}")
        elif cmp is None and "error" not in o:
            report.problems.append(f"{rel}: no canonical comparison ({d.get('canon_error') or d.get('metadata_error')})")
        if checked:
            report.cross_checked += 1
        out += [note, "", "query I", q_count(rel), "----", str(expected_count)]
        out += ["", "query II", q_describe(rel), "----"] + ["\t".join(c) for c in d["describe"]]
        if count > 0 and (checked or "error" in o):
            if count <= EXPLICIT_ROWS_MAX and renderable(d["rendered"]):
                report.explicit += 1
                letters = "".join(type_letter(t) for t in d["types"])
                out += ["", f"query {letters}", q_rows(rel), "----"] + ["\t".join(row) for row in d["rendered"]]
            elif count > EXPLICIT_ROWS_MAX and "summarize" in d and renderable(d["summarize"]):
                report.summarized += 1
                out += ["", f"query {SUMMARIZE_TYPES}", q_summarize(rel), "----"] + \
                       ["\t".join(row) for row in d["summarize"]]
            else:
                report.hashed += 1
                why = d.get("summarize_error", "a value does not fit on a result line").split("\n")[0]
                out += ["", f"# hashed: {why}", "query " + "I" * len(d["types"]), q_rows(rel), "----", d["hash"]]
        # the writer round trip
        rt = d["roundtrip"]
        if "error" in rt:
            report.roundtrip_refused += 1
            out += ["", "# the writer refuses this schema", "statement error", q_copy(rel), "----",
                    error_substring(rt["error"])]
            continue
        if "compare_error" in rt:
            report.problems.append(f"{rel}: round trip comparison failed: {rt['compare_error']}")
            continue
        if rt["written_minus_original"] or rt["original_minus_written"]:
            report.problems.append(f"{rel}: round trip differs: {rt}")
        out += ["", "statement ok", q_copy(rel)]
        if rt["as_text"]:
            report.roundtrip_text += 1
            out += ["", "# the written file reads back with other types (an Avro union is positional: a nullable",
                    "# UNION column comes back with its members named after their branch index), so the rows are",
                    "# compared as text",
                    "query II", q_describe_written(rel), "----"] + ["\t".join(c) for c in rt["describe"]]
        else:
            report.roundtrip_ok += 1
        for written_first in (True, False):
            out += ["", "query I", q_except(rel, written_first, rt["as_text"]), "----",
                    str(rt["written_minus_original"] if written_first else rt["original_minus_written"])]
    return "\n".join(out) + "\n"


def generate(args):
    manifest = read_manifest(MANIFEST)
    sources = {}
    for r in manifest:
        sources.setdefault(r["path"].split("/")[0], []).append(r)
    if args.only:
        sources = {k: v for k, v in sources.items() if k == args.only}
        if not sources:
            raise SystemExit(f"no source {args.only!r} in {MANIFEST}")
    os.makedirs(TESTS, exist_ok=True)
    workdir = tempfile.mkdtemp(prefix="gen_corpus_tests_")
    report = Report()
    extension = os.path.abspath(args.extension)
    if not os.path.exists(extension):
        raise SystemExit(f"{extension} not found: run make first")

    def measure(source):
        rows = sources[source]
        duck = run_worker(args.python, ["--duck-worker", extension],
                          {"files": [r["path"] for r in rows], "workdir": workdir, "timezone": "UTC"}, workdir,
                          source + ".duck")
        oracle = run_worker(args.oracle_python, ["--oracle-worker"],
                            {"files": {r["path"]: duck[r["path"]].get("canon_dump") for r in rows}}, workdir,
                            source + ".oracle")
        return source, duck, oracle

    with ThreadPoolExecutor(max_workers=args.jobs) as pool:
        measured = list(pool.map(measure, sorted(sources)))
    for source, duck, oracle in measured:
        text = emit_file(source, sources[source], duck, oracle, report)
        path = os.path.join(TESTS, source + ".test")
        if not os.path.exists(path) or open(path, encoding="utf-8").read() != text:
            with open(path, "w", encoding="utf-8") as f:
                f.write(text)
    if not args.only:
        for name in os.listdir(TESTS):
            if name.endswith(".test") and name[:-5] not in sources:
                os.unlink(os.path.join(TESTS, name))
    shutil.rmtree(workdir, ignore_errors=True)
    print(f"{len(sources)} test files for {report.files} corpus files: {report.refused} refused by the reader, "
          f"{report.explicit} with explicit rows, {report.summarized} summarized, {report.hashed} hashed; "
          f"{report.cross_checked} cross-checked against fastavro, {len(report.no_oracle)} without a fastavro reading; "
          f"round trips: {report.roundtrip_ok} identical, {report.roundtrip_text} compared as text, "
          f"{report.roundtrip_refused} refused by the writer")
    for line in report.no_oracle:
        print("  no fastavro reading:", line)
    for line in report.problems:
        print("  PROBLEM:", line)
    return 1 if report.problems else 0


def main():
    if len(sys.argv) > 1 and sys.argv[1] == "--duck-worker":
        duck_worker(*sys.argv[2:5])
        return
    if len(sys.argv) > 1 and sys.argv[1] == "--oracle-worker":
        oracle_worker(*sys.argv[2:4])
        return
    ap = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    ap.add_argument("--import", dest="import_manifest", metavar="MANIFEST_TSV",
                    help="select and copy the files of a corpus into test/corpus, then exit")
    ap.add_argument("--jobs", type=int, default=8, help="sources measured concurrently")
    ap.add_argument("--only", help="regenerate one source only")
    ap.add_argument("--python", default=os.path.join(ROOT, "venv", "bin", "python"),
                    help="interpreter with the pinned duckdb wheel")
    ap.add_argument("--oracle-python", default=os.environ.get("AVRO_ORACLE_PYTHON")
                    or os.path.join(ROOT, "venv", "bin", "python"),
                    help="interpreter with fastavro (and avro) installed")
    ap.add_argument("--extension", default=os.path.join(ROOT, "build", "avro.duckdb_extension"))
    args = ap.parse_args()
    if args.import_manifest:
        import_corpus(args.import_manifest)
        return
    sys.exit(generate(args))


if __name__ == "__main__":
    main()
