#!/usr/bin/env python3
"""A small sqllogictest runner on the DuckDB Python package, for test/sql of this repository.

    run_sqllogic.py [--init-sql SQL] [--config key=value ...] <files>

Supports the directives the tests use: `require <feature>` (`allow_unsigned_extensions` is granted, anything else
skips the file), `require-env VAR`, `statement ok` / `statement error` (with an expected message substring or a
`<REGEX>:` pattern after `----`), `query <types>` with the expected rows after `----` (tab-separated columns, `NULL`,
`(empty)`, a `<REGEX>:` cell), `foreach var a b c` / `endloop` with `{var}` or `${var}` substitution, and `{VAR}` /
`${VAR}` substitution of environment variables named by `require-env`. Results are rendered by DuckDB (every column
cast to VARCHAR), as the C++ runner does.
"""
import argparse, os, re, sys
import duckdb

def render(con, sql):
    rel = con.sql(sql)
    if rel is None:
        return []
    cols = rel.columns
    projected = rel.project(', '.join(f'CAST("{c}" AS VARCHAR) AS "c{i}"' for i, c in enumerate(cols)))
    rows = []
    for row in projected.fetchall():
        rows.append(['NULL' if v is None else ('(empty)' if v == '' else v) for v in row])
    return rows

def matches(expected, actual):
    if expected.startswith('<REGEX>:'):
        return re.fullmatch(expected[len('<REGEX>:'):], actual) is not None
    return expected == actual

def run_file(path, init_sql, config):
    lines = open(path).read().split('\n')
    env_vars = {}
    i = 0
    con = duckdb.connect(config=config)
    if init_sql:
        con.execute(init_sql)
    failures = 0
    checks = 0

    def substitute(text, loop_vars):
        for k, v in list(env_vars.items()) + list(loop_vars.items()):
            text = text.replace('${' + k + '}', v).replace('{' + k + '}', v)
        return text

    def read_block(i):
        block = []
        while i < len(lines) and lines[i].strip() != '':
            block.append(lines[i]); i += 1
        return block, i

    # expand loops into a flat list of (statement block lines, loop vars)
    def expand(start, end, loop_vars, out):
        j = start
        while j < end:
            line = lines[j]
            if line.startswith('foreach '):
                parts = line.split()
                var, values = parts[1], parts[2:]
                depth = 1; k = j + 1
                while k < end:
                    if lines[k].startswith('foreach '): depth += 1
                    if lines[k].startswith('endloop'):
                        depth -= 1
                        if depth == 0: break
                    k += 1
                for v in values:
                    expand(j + 1, k, {**loop_vars, var: v}, out)
                j = k + 1
                continue
            if line.strip() == '' or line.startswith('#'):
                j += 1; continue
            block, j = read_block(j)
            out.append((block, dict(loop_vars)))
        return out

    blocks = expand(0, len(lines), {}, [])
    for block, loop_vars in blocks:
        head = block[0]
        if head.startswith('require-env '):
            name = head.split()[1]
            if name not in os.environ:
                return 'skipped', f'require-env {name}'
            env_vars[name] = os.environ[name]
            continue
        if head.startswith('require '):
            feature = head.split()[1]
            if feature not in ('allow_unsigned_extensions',):
                return 'skipped', f'require {feature}'
            continue
        if head.startswith('statement ') or head.startswith('query '):
            kind = head.split()[0]
            expect_error = head.startswith('statement error')
            body = block[1:]
            if '----' in body:
                sep = body.index('----')
                sql_lines, expected = body[:sep], body[sep + 1:]
            else:
                sql_lines, expected = body, []
            sql = substitute('\n'.join(sql_lines), loop_vars)
            expected = [substitute(e, loop_vars) for e in expected]
            checks += 1
            try:
                if kind == 'query':
                    rows = render(con, sql)
                else:
                    con.execute(sql)
                    rows = None
            except Exception as e:  # noqa: BLE001 - the test decides whether an error is expected
                message = str(e)
                if expect_error:
                    want = '\n'.join(expected).strip()
                    if want and not (re.search(want[len('<REGEX>:'):], message) if want.startswith('<REGEX>:') else want in message):
                        failures += 1
                        print(f"{path}: expected error containing {want!r}, got: {message[:300]}")
                    continue
                failures += 1
                print(f"{path}: unexpected error for\n{sql}\n  {message[:400]}")
                continue
            if expect_error:
                failures += 1
                print(f"{path}: statement succeeded but an error was expected:\n{sql}")
                continue
            if kind == 'query':
                got = ['\t'.join(r) for r in rows]
                if len(got) != len(expected) or any(not all(matches(e, a) for e, a in zip(er.split('\t'), ar.split('\t'))) or er.count('\t') != ar.count('\t') for er, ar in zip(expected, got)):
                    failures += 1
                    print(f"{path}: result mismatch for\n{sql}\n  expected: {expected}\n  got:      {got}")
            continue
        # anything else (e.g. `mode`, `load`) is ignored
    return ('failed' if failures else 'passed'), f'{checks} checks, {failures} failures'

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--init-sql', default=os.environ.get('RMS_TEST_INIT_SQL', ''), help='SQL run on every connection first (e.g. LOAD httpfs; CREATE SECRET ...)')
    ap.add_argument('--config', action='append', default=[], help='DuckDB config option key=value')
    ap.add_argument('files', nargs='+')
    args = ap.parse_args()
    config = {'allow_unsigned_extensions': 'true'}
    for kv in args.config:
        k, v = kv.split('=', 1); config[k] = v
    outcome = {}
    for f in args.files:
        status, detail = run_file(f, args.init_sql, config)
        outcome[f] = status
        print(f'{status:8} {f} ({detail})')
    sys.exit(1 if 'failed' in outcome.values() else 0)

if __name__ == '__main__':
    main()
