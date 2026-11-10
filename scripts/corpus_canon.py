"""The canonical form a row of an Avro file takes on both sides of the corpus cross-check (scripts/gen_corpus_tests.py).

Taken from the harness of the real-world corpus run (avro-corpus/tools/avro_schema.py) and extended with the
`duration` logical type and decimals wider than 38 digits.

- parse(schema_json) -> Node tree (named-type references resolved, cycles detected)
- root_columns(root) -> the columns the reader exposes for a root schema
- duck_canon_sql(node, expr) -> SQL expression that turns the DuckDB value our reader produced for `node` into the
  canonical shape (temporal -> integer micro/nano/day counts, blob/fixed -> lowercase hex, decimal/uuid/enum -> text,
  float -> double, duration -> {months, days, micros}, multi-branch unions -> {t: 'u<i>', v: json})
- oracle_canon(node, value) -> the same canonical shape from a fastavro raw value (LOGICAL_READERS cleared,
  return_record_name=True, return_named_type=True)
"""
import json
import struct
import uuid

PRIMITIVES = {"null", "boolean", "int", "long", "float", "double", "bytes", "string"}


class RecursiveSchema(Exception):
    pass


class Node:
    __slots__ = ("kind", "name", "logical", "size", "precision", "scale", "fields", "items", "values", "branches",
                 "symbols", "attrs", "recursive_ref")

    def __init__(self, kind, **kw):
        self.kind = kind
        self.name = kw.get("name")
        self.logical = kw.get("logical")
        self.size = kw.get("size")
        self.precision = kw.get("precision")
        self.scale = kw.get("scale")
        self.fields = kw.get("fields")        # list of (name, Node, attrs)
        self.items = kw.get("items")
        self.values = kw.get("values")
        self.branches = kw.get("branches")
        self.symbols = kw.get("symbols")
        self.attrs = kw.get("attrs") or {}
        self.recursive_ref = False


def _full_name(name, namespace, enclosing):
    if "." in name:
        return name
    ns = namespace if namespace is not None else enclosing
    return f"{ns}.{name}" if ns else name


def parse(schema):
    named = {}
    stack = []  # names currently being defined (for cycle detection)
    recursion = []

    def walk(s, enclosing):
        if isinstance(s, str):
            if s in PRIMITIVES:
                return Node(s)
            fn = _full_name(s, None, enclosing)
            if fn in named:
                if fn in stack:
                    recursion.append(fn)
                    n = Node("ref", name=fn)
                    n.recursive_ref = True
                    return n
                return named[fn]
            if s in named:  # fully qualified already
                return named[s]
            raise ValueError(f"unknown type reference {s!r}")
        if isinstance(s, list):
            return Node("union", branches=[walk(b, enclosing) for b in s])
        if not isinstance(s, dict):
            raise ValueError(f"bad schema element {s!r}")
        t = s.get("type")
        attrs = {k: v for k, v in s.items() if k not in ("type", "fields", "items", "values", "symbols", "name",
                                                           "namespace", "size")}
        if isinstance(t, (dict, list)):
            n = walk(t, enclosing)
            if attrs:
                # logicalType etc. on a wrapper dict around a nested type definition
                m = Node(n.kind)
                for a in Node.__slots__:
                    setattr(m, a, getattr(n, a))
                m.attrs = {**n.attrs, **attrs}
                if "logicalType" in attrs and m.kind in PRIMITIVES | {"fixed"}:
                    m.logical = attrs["logicalType"]
                return m
            return n
        if t in PRIMITIVES:
            n = Node(t, attrs=attrs)
            n.logical = s.get("logicalType")
            n.precision = s.get("precision")
            n.scale = s.get("scale", 0 if n.logical == "decimal" else None)
            return n
        if t in ("record", "error", "enum", "fixed"):
            fn = _full_name(s["name"], s.get("namespace"), enclosing)
            ns = fn.rsplit(".", 1)[0] if "." in fn else None
            if t in ("record", "error"):
                n = Node("record", name=fn, fields=[], attrs=attrs)
                named[fn] = n
                stack.append(fn)
                for f in s.get("fields", []):
                    fattrs = {k: v for k, v in f.items() if k not in ("name", "type")}
                    n.fields.append((f["name"], walk(f["type"], ns), fattrs))
                stack.pop()
                return n
            if t == "enum":
                n = Node("enum", name=fn, symbols=list(s.get("symbols", [])), attrs=attrs)
                named[fn] = n
                return n
            n = Node("fixed", name=fn, size=s.get("size"), attrs=attrs)
            n.logical = s.get("logicalType")
            n.precision = s.get("precision")
            n.scale = s.get("scale", 0 if n.logical == "decimal" else None)
            named[fn] = n
            return n
        if t == "array":
            n = Node("array", items=walk(s["items"], enclosing), attrs=attrs)
            n.logical = s.get("logicalType")
            return n
        if t == "map":
            return Node("map", values=walk(s["values"], enclosing), attrs=attrs)
        # a reference given as {"type": "SomeName"}
        return walk(t, enclosing)

    root = walk(schema, None)
    return root, recursion


# ---------------------------------------------------------------- shape helpers

def non_null_branches(node):
    return [(i, b) for i, b in enumerate(node.branches) if b.kind != "null"]


def array_map_entry(node):
    """The key/value record of an array with logicalType map (Iceberg's MAP encoding, read as MAP), or None; the
    record may sit in a nullable union (BigQuery's export writes it so)."""
    if node.kind != "array" or node.logical != "map":
        return None
    it = node.items
    if it.kind == "union":
        nn = non_null_branches(it)
        if len(nn) != 1:
            return None
        it = nn[0][1]
    if it.kind == "record" and len(it.fields) == 2 and it.fields[0][0] == "key" and it.fields[1][0] == "value":
        return it
    return None


def is_array_map(node):
    return array_map_entry(node) is not None


def is_decimal(node):
    """A decimal the reader maps to DECIMAL: wider than 38 digits (or with invalid attributes) it reads the bytes."""
    p, s = node.precision, node.scale
    return (node.logical == "decimal" and isinstance(p, int) and isinstance(s, int) and 0 < p <= 38 and 0 <= s <= p)


def is_duration(node):
    return node.kind == "fixed" and node.logical == "duration" and node.size == 12


# ---------------------------------------------------------------- DuckDB side

def q(ident):
    return '"' + ident.replace('"', '""') + '"'


class _Lam:
    def __init__(self):
        self.n = 0

    def var(self):
        self.n += 1
        return f"x{self.n}"


def duck_canon_sql(node, expr, lam=None):
    lam = lam or _Lam()
    k = node.kind
    if k in ("null", "boolean", "int", "long", "string"):
        lt = node.logical
        if k == "int" and lt == "date":
            return f"date_diff('day', DATE '1970-01-01', {expr})"
        if k == "int" and lt == "time-millis":
            return f"epoch_us(DATE '1970-01-01' + {expr})"
        if k == "long" and lt == "time-micros":
            return f"epoch_us(DATE '1970-01-01' + {expr})"
        if k == "long" and lt in ("timestamp-millis", "timestamp-micros", "local-timestamp-millis", "local-timestamp-micros"):
            return f"epoch_us({expr})"
        if k == "long" and lt in ("timestamp-nanos", "local-timestamp-nanos"):
            return f"epoch_ns({expr})"
        return expr
    if k == "float":
        return f"({expr})::DOUBLE"
    if k == "double":
        return expr
    if k in ("bytes", "fixed"):
        if is_decimal(node):
            return f"({expr})::VARCHAR"
        if k == "fixed" and node.logical == "uuid":
            return f"({expr})::VARCHAR"
        if is_duration(node):
            # INTERVAL -> the three counts of the Avro duration (months, days, milliseconds -> microseconds)
            return (f"CASE WHEN ({expr}) IS NULL THEN NULL ELSE struct_pack("
                    f"months := datepart('year', {expr}) * 12 + datepart('month', {expr}), "
                    f"days := datepart('day', {expr}), "
                    f"micros := datepart('hour', {expr}) * 3600000000 + datepart('minute', {expr}) * 60000000 "
                    f"+ datepart('microsecond', {expr})) END")
        return f"lower(hex({expr}))"
    if k == "enum":
        return f"({expr})::VARCHAR"
    if k == "record":
        if not node.fields:
            return expr
        parts = []
        for fname, fnode, _ in node.fields:
            parts.append(f"{q(fname)} := {duck_canon_sql(fnode, f'struct_extract({expr}, {sql_str(fname)})', lam)}")
        return f"CASE WHEN ({expr}) IS NULL THEN NULL ELSE struct_pack({', '.join(parts)}) END"
    if k == "array":
        if is_array_map(node):
            vnode = array_map_entry(node).fields[1][1]
            v = lam.var()
            inner = duck_canon_sql(vnode, v, lam)
            if inner == v:
                return expr
            return f"CASE WHEN ({expr}) IS NULL THEN NULL ELSE map(map_keys({expr}), list_transform(map_values({expr}), lambda {v}: {inner})) END"
        v = lam.var()
        inner = duck_canon_sql(node.items, v, lam)
        if inner == v:
            return expr
        return f"list_transform({expr}, lambda {v}: {inner})"
    if k == "map":
        v = lam.var()
        inner = duck_canon_sql(node.values, v, lam)
        if inner == v:
            return expr
        return f"CASE WHEN ({expr}) IS NULL THEN NULL ELSE map(map_keys({expr}), list_transform(map_values({expr}), lambda {v}: {inner})) END"
    if k == "union":
        nn = non_null_branches(node)
        if len(nn) == 0:
            return expr
        if len(nn) == 1:
            return duck_canon_sql(nn[0][1], expr, lam)
        whens = []
        for i, b in nn:
            whens.append(f"WHEN 'u{i}' THEN to_json({duck_canon_sql(b, f'union_extract({expr}, {sql_str(f'u{i}')})', lam)})")
        return (f"CASE WHEN ({expr}) IS NULL THEN NULL ELSE struct_pack(t := union_tag({expr})::VARCHAR, "
                f"v := CASE union_tag({expr})::VARCHAR {' '.join(whens)} END) END")
    if k == "ref":
        raise RecursiveSchema(node.name)
    raise ValueError(f"unknown node kind {k}")


def sql_str(s):
    return "'" + s.replace("'", "''") + "'"


def root_columns(root):
    """(column name, node) pairs our reader exposes for this root schema, or None if root is a plain record."""
    node = root
    if node.kind == "union":
        nn = non_null_branches(node)
        if len(nn) == 1 and nn[0][1].kind == "record":
            node = nn[0][1]
    if node.kind == "record" and node.fields:
        return [(f, n) for f, n, _ in node.fields], node
    return None, node


# ---------------------------------------------------------------- oracle side

def _hex(b):
    if isinstance(b, (bytes, bytearray)):
        return bytes(b).hex()
    if isinstance(b, str):  # fastavro may hand back str for bytes in some paths
        return b.encode("latin-1", "replace").hex()
    raise TypeError(f"bytes expected, got {type(b)}")


def _decimal_str(b, scale):
    if isinstance(b, str):
        b = b.encode("latin-1")
    unscaled = int.from_bytes(bytes(b), "big", signed=True) if len(b) else 0
    if scale is None or scale == 0:
        return str(unscaled)
    sign = "-" if unscaled < 0 else ""
    digits = str(abs(unscaled)).rjust(scale + 1, "0")
    return f"{sign}{digits[:-scale]}.{digits[-scale:]}"


def _unwrap_named(value):
    # with return_record_name/return_named_type, union members that are named types come as (name, value)
    if isinstance(value, tuple) and len(value) == 2 and isinstance(value[0], str):
        return value[0], value[1]
    return None, value


def _branch_for(node, value):
    """Pick the union branch index for a raw fastavro value."""
    name, inner = _unwrap_named(value)
    cands = []
    for i, b in enumerate(node.branches):
        if name is not None:
            if b.kind in ("record", "enum", "fixed") and (b.name == name or b.name.rsplit(".", 1)[-1] == name
                                                            or name in (b.attrs.get("aliases") or [])):
                cands.append(i)
            continue
        if inner is None and b.kind == "null":
            cands.append(i)
        elif isinstance(inner, bool) and b.kind == "boolean":
            cands.append(i)
        elif isinstance(inner, int) and not isinstance(inner, bool) and b.kind in ("int", "long"):
            cands.append(i)
        elif isinstance(inner, float) and b.kind in ("float", "double"):
            cands.append(i)
        # fastavro names the named types (record, enum, fixed) of a union, so a bare value is an unnamed type
        elif isinstance(inner, str) and b.kind == "string":
            cands.append(i)
        elif isinstance(inner, (bytes, bytearray)) and b.kind == "bytes":
            cands.append(i)
        elif isinstance(inner, dict) and b.kind == "map":
            cands.append(i)
        elif isinstance(inner, list) and b.kind == "array":
            cands.append(i)
    return cands, inner


def oracle_canon(node, value):
    k = node.kind
    if k == "union":
        nn = non_null_branches(node)
        if value is None:
            return None
        if len(nn) == 1:
            _, inner = _unwrap_named(value)
            return oracle_canon(nn[0][1], inner)
        cands, inner = _branch_for(node, value)
        if not cands:
            raise ValueError(f"no union branch for value {value!r:.80}")
        i = cands[0]
        return {"t": f"u{i}", "v": oracle_canon(node.branches[i], inner), "_alt": [f"u{j}" for j in cands[1:]]}
    if value is None:
        return None
    if k == "null":
        return None
    if k == "boolean":
        return bool(value)
    if k == "int":
        if node.logical == "time-millis":
            return int(value) * 1000
        return int(value)
    if k == "long":
        if node.logical in ("timestamp-millis", "local-timestamp-millis"):
            return int(value) * 1000
        return int(value)
    if k in ("float", "double"):
        return float(value)
    if k == "string":
        return value if isinstance(value, str) else value.decode("utf-8")
    if k in ("bytes", "fixed"):
        _, value = _unwrap_named(value)
        if is_decimal(node):
            return _decimal_str(value, node.scale)
        if k == "fixed" and node.logical == "uuid":
            return str(uuid.UUID(bytes=bytes(value)))
        if is_duration(node):
            months, days, millis = struct.unpack("<III", bytes(value))
            return {"months": months, "days": days, "micros": millis * 1000}
        return _hex(value)
    if k == "enum":
        _, value = _unwrap_named(value)
        return value
    if k == "record":
        _, value = _unwrap_named(value)
        if not node.fields:
            return None
        return {f: oracle_canon(n, value.get(f)) for f, n, _ in node.fields}
    if k == "array":
        if is_array_map(node):
            entry = array_map_entry(node)
            knode, vnode = entry.fields[0][1], entry.fields[1][1]
            out = {}
            for e in value:
                _, e = _unwrap_named(e)
                if e is None:
                    continue  # a null entry adds nothing to the MAP
                kk = oracle_canon(knode, e["key"])
                out[kk if isinstance(kk, str) else json.dumps(kk)] = oracle_canon(vnode, e["value"])
            return out
        return [oracle_canon(node.items, v) for v in value]
    if k == "map":
        return {kk: oracle_canon(node.values, v) for kk, v in value.items()}
    if k == "ref":
        raise RecursiveSchema(node.name)
    raise ValueError(k)
