#!/usr/bin/env -S python3 -I
"""Reorders the C API v2 function table of crates/duckdb-capi-sys to the member order of a DuckDB header.

    pin_api_table.py <path to duckdb_extension_v2.h of the DuckDB build to pin>

The engine hands extensions a struct of function pointers (`duckdb_ext_api_v2`); its layout is the header's member
order, which changes between DuckDB lines while the C API v2 is unstable. The Rust struct's fields and the name
tables must follow the pinned build. Entries the header lacks keep their relative order after the header's; those that stood
before `duckdb_v2_library_api_table_size` (entries of a newer upstream header the pinned one does not have yet) go
to the very end, behind the appended band, so that UPSTREAM_API_TABLE_SIZE (the offset of that entry) stays exactly
the pinned header's table size and the band keeps its slots."""
import re, sys, pathlib

header = pathlib.Path(sys.argv[1]).read_text()
m = re.search(r'typedef struct \{\n(.*?)\n\} duckdb_ext_api_v2;', header, re.S)
if not m:
    sys.exit("no duckdb_ext_api_v2 struct in the header")
order = re.findall(r'\(\*(duckdb_v2_[a-z0-9_]+)\)', m.group(1))
if len(order) != len(set(order)):
    sys.exit("duplicate member names in the header")
rank = {name: i for i, name in enumerate(order)}
BAND_START = 'duckdb_v2_library_api_table_size'

def split_appended(names):
    """The names the header lacks: (the band from BAND_START on, those that stood before it)."""
    rest = [n for n in names if n not in rank]
    if BAND_START not in rest:
        return rest, []
    seen_band = False
    band, demoted = [], []
    for n in rest:
        seen_band = seen_band or n == BAND_START
        (band if seen_band else demoted).append(n)
    return band, demoted

lib = pathlib.Path(__file__).resolve().parent.parent / 'crates' / 'duckdb-capi-sys' / 'src' / 'lib.rs'
src = lib.read_text()

def reorder_struct(src):
    start = src.index('pub struct duckdb_ext_api_v2 {\n') + len('pub struct duckdb_ext_api_v2 {\n')
    end = src.index('\n}\n', start)
    body = src[start:end]
    # a field starts at "    pub <name>:" and runs to the next field start
    starts = [mm.start() for mm in re.finditer(r'^    pub (duckdb_v2_[a-z0-9_]+):', body, re.M)]
    fields = []
    for i, s in enumerate(starts):
        e = starts[i + 1] if i + 1 < len(starts) else len(body)
        text = body[s:e]
        name = re.match(r'    pub (duckdb_v2_[a-z0-9_]+):', text).group(1)
        fields.append((name, text.rstrip('\n') + '\n'))
    names = [n for n, _ in fields]
    missing = [n for n in order if n not in names]
    if missing:
        sys.exit(f"the header names {len(missing)} entries the Rust struct lacks, e.g. {missing[:3]}: regenerate the bindings")
    upstream = sorted((f for f in fields if f[0] in rank), key=lambda f: rank[f[0]])
    band, demoted = split_appended(names)
    text_of = dict(fields)
    appended = [(n, text_of[n]) for n in band + demoted]
    new_body = ''.join(t for _, t in upstream + appended).rstrip('\n')
    return src[:start] + new_body + src[end:], len(upstream), len(band), len(demoted)

def reorder_names(src, const):
    mm = re.search(r'pub const ' + const + r': \[&str; (\d+)\] = \[\n(.*?)\n\];', src, re.S)
    if not mm:
        return src
    names = re.findall(r'"(duckdb_v2_[a-z0-9_]+)"', mm.group(2))
    upstream = sorted((n for n in names if n in rank), key=lambda n: rank[n])
    band, demoted = split_appended(names)
    body = '\n'.join(f'    "{n}",' for n in upstream + band + demoted)
    return src[:mm.start(2)] + body + src[mm.end(2):]

src, n_up, n_app, n_dem = reorder_struct(src)
for const in ('DUCKDB_V2_FUNCTION_NAMES', 'API_TABLE_FUNCTION_NAMES'):
    src = reorder_names(src, const)
lib.write_text(src)
print(f"header: {len(order)} entries; struct: {n_up} in header order + {n_app} appended"
      + (f" + {n_dem} the header lacks, moved behind the appended band" if n_dem else ""))
