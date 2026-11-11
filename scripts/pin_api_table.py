#!/usr/bin/env -S python3 -I
"""Reorders the C API v2 function table of crates/duckdb-capi-sys to the member order of a DuckDB header.

    pin_api_table.py <path to duckdb_extension_v2.h of the DuckDB build to pin> [--check-signatures]

The engine hands extensions a struct of function pointers (`duckdb_ext_api_v2`); its layout is the header's member
order, which changes between DuckDB lines while the C API v2 is unstable. The Rust struct's fields and the name
tables must follow the pinned build. Entries the header lacks keep their relative order after the header's; those that stood
before `duckdb_v2_library_api_table_size` (entries of a newer upstream header the pinned one does not have yet) go
to the very end, behind the appended band, so that UPSTREAM_API_TABLE_SIZE (the offset of that entry) stays exactly
the pinned header's table size and the band keeps its slots.

The order is all the script changes. With --check-signatures it then compares the return and parameter types of every
header entry with the Rust field of that name (C types spelled the Rust way: `const char **` is `*mut *const c_char`)
and exits non-zero listing the entries that differ; those bindings, and the wrappers calling them, need editing."""
import re, sys, pathlib

args = [a for a in sys.argv[1:] if a != '--check-signatures']
check_signatures = len(args) != len(sys.argv) - 1
if len(args) != 1:
    sys.exit(__doc__)
header = pathlib.Path(args[0]).read_text()
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

C_TO_RUST = {'char': 'c_char', 'void': 'c_void', 'int': 'c_int', 'double': 'f64', 'float': 'f32', 'size_t': 'usize',
             **{f'{u}int{b}_t': f'{"u" if u else "i"}{b}' for u in ('', 'u') for b in (8, 16, 32, 64)}}

def c_type(text, named):
    """A C type, or parameter declaration when `named`, the way Rust spells it."""
    tokens = [t for t in re.findall(r'[A-Za-z_][A-Za-z0-9_]*|\*', text) if t != 'struct']
    base = next(t for t in tokens if t != 'const')
    at = tokens.index(base)
    rest = tokens[at + 1:]
    if named and rest and rest[-1] not in ('*', 'const'):
        rest = rest[:-1]
    # const-ness per level: the base type, then every pointer; a pointer points at the level before it
    levels = ['const' in tokens[:at]]
    for t in rest:
        if t == '*':
            levels.append(False)
        else:
            levels[-1] = True
    out = C_TO_RUST.get(base, base)
    for pointee_const in levels[:-1]:
        out = ('*const ' if pointee_const else '*mut ') + out
    return out

def header_signatures():
    sigs = {}
    for ret, name, params in re.findall(r'([A-Za-z_][A-Za-z0-9_ *]*?)\s*\(\*(duckdb_v2_[a-z0-9_]+)\)\s*\(([^;]*)\);', m.group(1)):
        params = [] if params.strip() in ('', 'void') else [c_type(p, True) for p in params.split(',')]
        sigs[name] = (params, None if ret.strip() == 'void' else c_type(ret, False))
    return sigs

def rust_signatures(src):
    start = src.index('pub struct duckdb_ext_api_v2 {\n')
    body = src[start:src.index('\n}\n', start)]
    sigs = {}
    for name, params, ret in re.findall(
            r'pub (duckdb_v2_[a-z0-9_]+):\s*Option<\s*unsafe extern "C" fn\(([^()]*)\)(?:\s*->\s*([^,>]+?))?,?\s*>,', body):
        sigs[name] = ([' '.join(p.split()) for p in params.split(',') if p.strip()], ret or None)
    return sigs

if check_signatures:
    c_sigs, rust_sigs = header_signatures(), rust_signatures(src)
    unparsed = [n for n in order if n not in c_sigs or n not in rust_sigs]
    if unparsed:
        sys.exit(f"--check-signatures could not read {len(unparsed)} entries, e.g. {unparsed[:3]}")
    differing = [n for n in order if c_sigs[n] != rust_sigs[n]]
    for n in differing:
        print(f"{n}:\n  header: {c_sigs[n]}\n  rust:   {rust_sigs[n]}")
    if differing:
        sys.exit(f"{len(differing)} of {len(order)} signatures differ from the header")
    print(f"signatures: {len(order)} entries compared (return type and {sum(len(c_sigs[n][0]) for n in order)} "
          f"parameter types), all equal to the header's")
