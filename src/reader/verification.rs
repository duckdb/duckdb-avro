//! Kani proof harnesses (`cargo kani -p duckdb-avro`, `make kani`) over the row decoder: [`decode_rows`] and
//! [`Node::decode`], the pure half of a scan between the decompressed block and the engine's vectors, on concrete
//! small schemas with every byte string up to a bound as the block.
//!
//! Each harness fixes a schema (built node by node, as `root_columns` would read it) and the node tree that mirrors
//! it, hands the row decoder a symbolic byte slice and a symbolic row count, and decodes the same bytes a second time
//! with the primitive `Decoder` calls in schema order (the readers proven in `avro-core`), which is the specification:
//! the decoder succeeds exactly when every claimed row decodes, the buffers then hold exactly the reference's values
//! (a NULL on the validity side, a list's offsets and lengths, a union's tags) and the decoder stands where the
//! reference stands; on an error the buffers hold the rows before the failing one and at most the fields of that row
//! decoded before the failure, never a value past the claimed rows. The node tree is built by hand (the schema walk
//! that would build it needs the engine's type constructors); the schema arena is a [`Schema::unchecked`] with the
//! minimum encoded sizes stated (its validation hashes names with `RandomState`, out of CBMC's reach).
//!
//! Every node, schema node, field and id list lives in an array on the harness's stack, and the tree's `Vec`s and
//! `Box`es are pointers into those arrays ([`leaked`], [`leaked_box`]) that are forgotten, never freed: CBMC
//! propagates the variant of an enum it can see on the stack and prunes the arms of every `match` on it, while the
//! same enum behind a heap allocation keeps every arm and every recursive call of `Node::decode` and `skip` alive
//! (a one-node tree then runs for more than ten minutes; on the stack it takes seconds). The row count of a harness
//! is concrete (a symbolic `1..=2` costs CBMC two orders of magnitude more than running the two-row case, which
//! decodes a one-row block as its prefix), the reference is written out row by row into separate variables (an
//! array indexed by the number of rows decoded so far triples the time), and the item counts and byte lengths stay
//! small; where the input could run a collection longer, the reference assumes the block counts it reads within the
//! bound, which restricts the inputs to those the bound names. Results and schemas are forgotten rather than
//! dropped: their drop glue (recursive through `Json`, over a `Vec` whose length is unconstrained in the `Ok` branch)
//! is no part of the property.
//!
//! Each shape is a generic body over the block's byte bound and the row count; two proofs instantiate it: a small one
//! the CI job runs (one row, or one item block) and, for the record shapes, the two-row bound behind the `kani-heavy`
//! feature, which `make kani` enables (two to nine minutes and up to 16 GB each); the nullable field and the projection
//! have only a heavy one-row proof. The array, map and union shapes are beyond CBMC (see the notes in their place). The harnesses with a `string` column stand
//! [`std::str::from_utf8`] in with the byte-sequence table of the Unicode standard ([`utf8_by_table`], `-Z stubbing`):
//! `core`'s validator costs CBMC minutes per symbolic string, and `read_string_rejects_invalid_utf8_without_panicking`
//! in `avro-core` proves the two accept exactly the same byte strings. The table check loops over the string's bytes,
//! so those harnesses unwind one more than their byte bound; the others unwind 12 (a varint of 10 bytes).

use std::mem::ManuallyDrop;

use avro_core::{Decoder, Field, Name, Schema, SchemaId, SchemaKind, SchemaNode};

use super::decode_rows;
use super::node::{EnumValues, Node, Prim, Strings};
use crate::verification::well_formed_utf8;

/// A slice of symbolic bytes and symbolic length `0..=N`.
fn any_slice<const N: usize>(buf: &[u8; N]) -> &[u8] {
    let len: usize = kani::any();
    kani::assume(len <= N);
    &buf[..len]
}

/// A `Vec` over `items`, an array on the harness's stack that is never dropped; the vector is forgotten with the
/// tree, never freed or grown.
fn leaked<T, const N: usize>(items: &mut ManuallyDrop<[T; N]>) -> Vec<T> {
    // SAFETY: the vector is forgotten (never freed) and never grown; the array is never dropped and outlives every use
    // of the tree, and the harness reads the array again only after the decoder has returned
    unsafe { Vec::from_raw_parts(items.as_mut_ptr(), N, N) }
}

/// A `Box` over `node`, which lives on the harness's stack and is never dropped; the box is forgotten with the tree.
fn leaked_box(node: &mut ManuallyDrop<Node>) -> Box<Node> {
    // SAFETY: as for `leaked`
    unsafe { Box::from_raw(&mut **node) }
}

/// Stack storage for a tree node or an array of them that is never dropped (what [`leaked`] and [`leaked_box`] point
/// into, and what the top-level nodes are: a dropped node tree would free the leaked vectors).
fn pinned<T>(value: T) -> ManuallyDrop<T> {
    ManuallyDrop::new(value)
}

fn id(index: usize) -> SchemaId {
    SchemaId::from_index(index)
}

fn leaf(kind: SchemaKind) -> SchemaNode {
    SchemaNode::new(kind)
}

/// A record node over `fields`; its name is empty, like every field name in the harnesses: names play no part in
/// decoding, and an empty `String` allocates nothing.
fn record(fields: Vec<Field>) -> SchemaNode {
    SchemaNode::new(SchemaKind::Record { name: Name::simple(""), fields, is_error: false })
}

/// A field of type `ty` (unnamed, see [`record`]).
fn field(ty: SchemaId) -> Field {
    Field::new("", ty)
}

/// Room for the rows and bytes of any harness here: the buffers start with the capacity a previous chunk left them
/// (`Node::clear` keeps it), so that no append of a symbolic length reallocates (minutes of CBMC time each).
const ROOM: usize = 8;
const BYTES: usize = 32;

fn int() -> Node {
    Node::Int(Prim::with_capacity(ROOM))
}

fn long() -> Node {
    Node::Long(Prim::with_capacity(ROOM))
}

fn boolean() -> Node {
    Node::Boolean(Prim::with_capacity(ROOM))
}

fn string() -> Node {
    Node::String(Strings::with_capacity(ROOM, BYTES))
}

fn ints(node: &Node) -> (&[i32], &[usize]) {
    let Node::Int(p) = node else { panic!("not an int node") };
    p.buffers()
}

fn longs(node: &Node) -> (&[i64], &[usize]) {
    let Node::Long(p) = node else { panic!("not a long node") };
    p.buffers()
}

fn booleans(node: &Node) -> (&[u8], &[usize]) {
    let Node::Boolean(p) = node else { panic!("not a boolean node") };
    p.buffers()
}

fn strings(node: &Node) -> &Strings {
    match node {
        Node::String(s) | Node::Bytes(s) | Node::Fixed { values: s, .. } => s,
        _ => panic!("not a string node"),
    }
}

/// Stands in for [`std::str::from_utf8`] in the harnesses with a `string` column: well-formed UTF-8 by the
/// byte-sequence table of the Unicode standard (Table 3-7), which `avro-core`'s `read_string` harness proves
/// equivalent to `core`'s validator, at a fraction of its cost to CBMC. The error value comes from the validator's
/// `_mut` twin on one invalid byte (the type has no public constructor).
#[allow(dead_code, invalid_from_utf8)] // a stub is used by `#[kani::stub]` attributes only; the literal is meant to fail
fn utf8_by_table(bytes: &[u8]) -> Result<&str, std::str::Utf8Error> {
    if well_formed_utf8(bytes) {
        // SAFETY: `well_formed_utf8` is the UTF-8 definition, checked just above
        Ok(unsafe { std::str::from_utf8_unchecked(bytes) })
    } else {
        Err(std::str::from_utf8_mut(&mut [0xff]).unwrap_err())
    }
}

/// Stands in for [`Decoder::skip`] in the harness whose only skipped value is a union's `null` branch, which the
/// row decoder reaches through `branch_ids[selected]`: the index is symbolic, so the schema node is to CBMC any node
/// of the arena and every arm of the recursive `skip` stays alive (ten minutes and more for a one-row block). The
/// stand-in checks that the node skipped is indeed `null` and, like `skip` of `null`, reads nothing.
#[allow(dead_code)] // used by `#[kani::stub]` attributes only
fn skip_only_null<'a>(_dec: &mut Decoder<'a>, schema: &Schema, id: SchemaId) -> avro_core::Result<()>
where
    // the bound makes `'a` early-bound like the impl's, so the stub has the method's one generic parameter
    Decoder<'a>: Sized,
{
    assert!(matches!(schema[id].kind, SchemaKind::Null));
    Ok(())
}

/// Decodes `rows` rows of the record root `schema` into `nodes` through `projection`; returns whether it succeeded
/// and the decoder's final position (the nodes are kept for the assertions, the result forgotten).
fn decode(
    data: &[u8],
    schema: &Schema,
    nodes: &mut [Node],
    projection: &[Option<usize>],
    rows: usize,
) -> (bool, usize) {
    let mut dec = Decoder::new(data);
    let result = decode_rows(&mut dec, schema, true, nodes, projection, rows);
    let ok = result.is_ok();
    std::mem::forget(result);
    (ok, dec.position())
}

/// The reference's NULL rows of a two-row column: the rows (in order) whose flag is set.
fn null_rows(first: bool, second: bool) -> &'static [usize] {
    match (first, second) {
        (false, false) => &[],
        (true, false) => &[0],
        (false, true) => &[1],
        (true, true) => &[0, 1],
    }
}

/// The reference's second row: read when the harness decodes two rows and the first one was complete.
fn second_row<T>(rows: usize, row0: &Option<T>, mut row: impl FnMut() -> Option<T>) -> Option<T> {
    if rows > 1 && row0.is_some() { row() } else { None }
}

/// Whether every claimed row decoded and how many did.
fn outcome<T>(rows: usize, row0: &Option<T>, row1: &Option<T>) -> (bool, usize) {
    let complete = usize::from(row0.is_some()) + usize::from(row1.is_some());
    (complete == rows, complete)
}

/// 1. A flat record `{a: int, b: long, c: boolean}`, `ROWS` rows, blocks of at most `N` bytes: the three buffers
/// hold the reference's values of every complete row, nothing is NULL, success exactly when every row decodes.
fn flat_record<const N: usize, const ROWS: usize>() {
    fn row(dec: &mut Decoder<'_>) -> Option<(i32, i64, bool)> {
        match (dec.read_int(), dec.read_long(), dec.read_boolean()) {
            (Ok(a), Ok(b), Ok(c)) => Some((a, b, c)),
            _ => None,
        }
    }
    let buf: [u8; N] = kani::any();
    let data = any_slice(&buf);
    let mut reference = Decoder::new(data);
    let row0 = row(&mut reference);
    let row1 = second_row(ROWS, &row0, || row(&mut reference));
    let (ok, complete) = outcome(ROWS, &row0, &row1);

    let mut fields = pinned([field(id(0)), field(id(1)), field(id(2))]);
    let mut arena =
        pinned([leaf(SchemaKind::Int), leaf(SchemaKind::Long), leaf(SchemaKind::Boolean), record(leaked(&mut fields))]);
    let mut sizes = pinned([1, 1, 1, 3]);
    let schema = Schema::unchecked(leaked(&mut arena), id(3), leaked(&mut sizes));
    let mut nodes = pinned([int(), long(), boolean()]);
    let (decoded, position) = decode(data, &schema, &mut *nodes, &[Some(0), Some(1), Some(2)], ROWS);

    assert_eq!(decoded, ok);
    let (va, na) = ints(&nodes[0]);
    let (vb, nb) = longs(&nodes[1]);
    let (vc, nc) = booleans(&nodes[2]);
    assert!(na.is_empty() && nb.is_empty() && nc.is_empty());
    if ok {
        assert_eq!(position, reference.position());
    }
    // a failing row holds the fields decoded before the failure, in field order
    assert!(va.len() <= complete + 1 && vb.len() <= va.len() && vc.len() <= vb.len() && vc.len() >= complete);
    if let Some((a, b, c)) = row0 {
        assert!(va[0] == a && vb[0] == b && vc[0] == u8::from(c));
    }
    if let Some((a, b, c)) = row1 {
        assert!(va[1] == a && vb[1] == b && vc[1] == u8::from(c));
    }
    std::mem::forget(schema);
}

#[kani::proof]
#[kani::unwind(12)]
fn flat_record_one_row_matches_the_reference() {
    flat_record::<12, 1>();
}

#[cfg(feature = "kani-heavy")]
#[kani::proof]
#[kani::unwind(12)]
fn flat_record_rows_match_the_reference() {
    flat_record::<24, 2>();
}

/// 2. A nullable field beside a string, `{a: ["null", "int"], b: string}` (strings of at most 4 bytes), `ROWS`
/// rows, blocks of at most `N` bytes: a null branch becomes a NULL row (placeholder value, row in the validity
/// list), the int branch the value; the string rows are the reference's bytes.
fn nullable_field<const N: usize, const ROWS: usize>() {
    fn row<'a>(dec: &mut Decoder<'a>) -> Option<(Option<i32>, &'a [u8])> {
        let a = match dec.read_union_index(2) {
            Ok(0) => Ok(None),
            Ok(_) => dec.read_int().map(Some),
            Err(e) => Err(e),
        };
        match (a, dec.read_string()) {
            (Ok(a), Ok(b)) => {
                kani::assume(b.len() <= 4);
                Some((a, b.as_bytes()))
            }
            _ => None,
        }
    }
    let buf: [u8; N] = kani::any();
    let data = any_slice(&buf);
    let mut reference = Decoder::new(data);
    let row0 = row(&mut reference);
    let row1 = second_row(ROWS, &row0, || row(&mut reference));
    let (ok, complete) = outcome(ROWS, &row0, &row1);

    // 0 null, 1 int, 2 the union, 3 string, 4 the record
    let mut branches = pinned([id(0), id(1)]);
    let mut fields = pinned([field(id(2)), field(id(3))]);
    let mut arena = pinned([
        leaf(SchemaKind::Null),
        leaf(SchemaKind::Int),
        leaf(SchemaKind::Union { branches: leaked(&mut branches) }),
        leaf(SchemaKind::String),
        record(leaked(&mut fields)),
    ]);
    let mut sizes = pinned([0, 1, 1, 1, 2]);
    let schema = Schema::unchecked(leaked(&mut arena), id(4), leaked(&mut sizes));
    let mut branch_ids = pinned([id(0), id(1)]);
    let mut child = pinned(int());
    let mut nodes = pinned([
        Node::Nullable { branch_ids: leaked(&mut branch_ids), branch: 1, child: leaked_box(&mut child) },
        string(),
    ]);
    let (decoded, position) = decode(data, &schema, &mut *nodes, &[Some(0), Some(1)], ROWS);

    assert_eq!(decoded, ok);
    let (va, na) = ints(&child);
    let sb = strings(&nodes[1]);
    if ok {
        assert_eq!(position, reference.position());
    }
    assert!(va.len() <= complete + 1 && sb.len() <= va.len() && sb.len() >= complete);
    assert!(sb.nulls().is_empty());
    let null0 = matches!(row0, Some((None, _)));
    let null1 = matches!(row1, Some((None, _)));
    if let Some((a, b)) = row0 {
        assert_eq!(va[0], a.unwrap_or(0));
        assert_eq!(sb.row(0), b);
    }
    if let Some((a, b)) = row1 {
        assert_eq!(va[1], a.unwrap_or(0));
        assert_eq!(sb.row(1), b);
    }
    let expected = null_rows(null0, null1);
    if va.len() == complete {
        assert_eq!(na, expected);
    } else {
        // the failing row's `a` is in (a value or a NULL) and its string failed
        assert!(!ok && na.len() >= expected.len() && na.len() <= expected.len() + 1);
        assert_eq!(&na[..expected.len()], expected);
        if na.len() > expected.len() {
            assert_eq!(na[expected.len()], complete);
        }
    }
    std::mem::forget(schema);
}

#[cfg(feature = "kani-heavy")]
#[kani::proof]
#[kani::unwind(12)]
#[kani::stub(std::str::from_utf8, utf8_by_table)]
#[kani::stub(avro_core::Decoder::skip, skip_only_null)]
fn nullable_field_null_goes_to_the_validity_side() {
    // nine minutes and 14 GB at one row: `kani-heavy`; two rows of 16 bytes exceed fifteen minutes
    nullable_field::<10, 1>();
}

// 3. and 4. An array field `{xs: array<int>}` and a map field `{m: map<long>}` were written the same way (one row, the
// reference reading the item blocks with `read_block_count` and assuming at most 3 items per block, the list entry
// `(0, item count)` and the child buffers compared item by item, also when the row fails part-way) and are beyond
// CBMC even at one block of at most 3 items in a 12-byte block with varints of at most 4 bytes and every loop unwound
// 5 times: the array ran out of 80 GB, the map past ten minutes. The item loop of `Node::decode` is unrolled once per
// unwinding with a full `read_int` (or key `read_string` and `read_long`) in each copy, on top of the block loop, and
// the row's bytes are shared by every copy. The block count's bound (`read_block_count`) and the primitive readers are
// proven in `avro-core`; the list and map assembly stays with the sqllogictests and the corpus.

// 5. A union field `{u: ["int", "string", "null"]}` was written the same way (the tag is the member index, 0 for a
// null row, which is NULL in the validity list; the selected member holds the value and every other member a NULL;
// `Decoder::skip` stood in by `skip_only_null`, `from_utf8` by the table) and is beyond CBMC: one row of 8 bytes
// exceeds twenty minutes. The three branches each read a different member and push NULLs into the others, and every
// branch stays live for every row; the union's tag and member numbering is proven in `crate::verification`
// (`union_layout`), the union assembly stays with the sqllogictests and the corpus.

/// 6. A nested record and an empty record beside an int, `{o: {x: int, y: int}, e: {}, n: int}`, `ROWS` rows,
/// blocks of at most `N` bytes: the STRUCT's two field buffers hold the reference's values and its row count grows
/// with the rows, the zero-width empty record counts its rows without reading a byte, the int follows.
fn nested_and_empty_records<const N: usize, const ROWS: usize>() {
    fn row(dec: &mut Decoder<'_>) -> Option<(i32, i32, i32)> {
        match (dec.read_int(), dec.read_int(), dec.read_int()) {
            (Ok(x), Ok(y), Ok(n)) => Some((x, y, n)),
            _ => None,
        }
    }
    let buf: [u8; N] = kani::any();
    let data = any_slice(&buf);
    let mut reference = Decoder::new(data);
    let row0 = row(&mut reference);
    let row1 = second_row(ROWS, &row0, || row(&mut reference));
    let (ok, complete) = outcome(ROWS, &row0, &row1);

    // 0 int, 1 the inner record, 2 the empty record, 3 the root record
    let mut inner_fields = pinned([field(id(0)), field(id(0))]);
    let mut root_fields = pinned([field(id(1)), field(id(2)), field(id(0))]);
    let mut arena = pinned([
        leaf(SchemaKind::Int),
        record(leaked(&mut inner_fields)),
        record(Vec::new()),
        record(leaked(&mut root_fields)),
    ]);
    let mut sizes = pinned([1, 2, 0, 3]);
    let schema = Schema::unchecked(leaked(&mut arena), id(3), leaked(&mut sizes));
    let mut inner = pinned([int(), int()]);
    let mut nodes = pinned([
        Node::Struct { fields: leaked(&mut inner), len: 0, nulls: Vec::with_capacity(ROOM) },
        Node::Null { id: id(2), len: 0 },
        int(),
    ]);
    let (decoded, position) = decode(data, &schema, &mut *nodes, &[Some(0), Some(1), Some(2)], ROWS);

    assert_eq!(decoded, ok);
    let Node::Struct { len, nulls, .. } = &nodes[0] else { panic!() };
    let Node::Null { len: empty_len, .. } = &nodes[1] else { panic!() };
    let (vx, nx) = ints(&inner[0]);
    let (vy, ny) = ints(&inner[1]);
    let (vn, nn) = ints(&nodes[2]);
    assert!(nulls.is_empty() && nx.is_empty() && ny.is_empty() && nn.is_empty());
    if ok {
        assert_eq!(position, reference.position());
    }
    // the STRUCT counts a row once both fields are in; the empty record and the int follow
    assert!(vx.len() <= complete + 1 && vy.len() <= vx.len() && *len <= vy.len() && *empty_len <= *len);
    assert!(vn.len() <= *empty_len && vn.len() >= complete);
    if let Some((x, y, n)) = row0 {
        assert!(vx[0] == x && vy[0] == y && vn[0] == n);
    }
    if let Some((x, y, n)) = row1 {
        assert!(vx[1] == x && vy[1] == y && vn[1] == n);
    }
    std::mem::forget(schema);
}

#[kani::proof]
#[kani::unwind(12)]
fn nested_and_empty_records_one_row_match_the_reference() {
    nested_and_empty_records::<10, 1>();
}

#[cfg(feature = "kani-heavy")]
#[kani::proof]
#[kani::unwind(12)]
fn nested_and_empty_records_match_the_reference() {
    nested_and_empty_records::<16, 2>();
}

/// 7. Projection: of `{a: int, b: string, c: double}` (strings of at most 4 bytes) only `b` is read, `ROWS` rows,
/// blocks of at most `N` bytes: the skipped `a` and `c` advance the decoder exactly as reading them does, so the
/// string buffer holds the reference's `b` of every row and the decoder stands where the reference stands.
fn projection<const N: usize, const ROWS: usize>() {
    /// The skipped fields are read as `skip` reads them: a skipped `int` is any varint (`skip` validates nothing, so a
    /// varint beyond i32, which `read_int` refuses, passes), a skipped `double` any 8 bytes.
    fn row<'a>(dec: &mut Decoder<'a>) -> Option<&'a [u8]> {
        match (dec.read_varint(), dec.read_string(), dec.read_raw(8, "")) {
            (Ok(_), Ok(b), Ok(_)) => {
                kani::assume(b.len() <= 4);
                Some(b.as_bytes())
            }
            _ => None,
        }
    }
    let buf: [u8; N] = kani::any();
    let data = any_slice(&buf);
    let mut reference = Decoder::new(data);
    let row0 = row(&mut reference);
    let row1 = second_row(ROWS, &row0, || row(&mut reference));
    let (ok, complete) = outcome(ROWS, &row0, &row1);

    let mut fields = pinned([field(id(0)), field(id(1)), field(id(2))]);
    let mut arena = pinned([
        leaf(SchemaKind::Int),
        leaf(SchemaKind::String),
        leaf(SchemaKind::Double),
        record(leaked(&mut fields)),
    ]);
    let mut sizes = pinned([1, 1, 8, 10]);
    let schema = Schema::unchecked(leaked(&mut arena), id(3), leaked(&mut sizes));
    let mut nodes = pinned([string()]);
    let (decoded, position) = decode(data, &schema, &mut *nodes, &[None, Some(0), None], ROWS);

    // skipping a leaf never fails where reading it succeeds, nor the other way round (the skip harnesses of avro-core
    // prove it per kind; the row decoder inherits it)
    assert_eq!(decoded, ok);
    let sb = strings(&nodes[0]);
    assert!(sb.nulls().is_empty());
    if ok {
        assert_eq!(position, reference.position());
    }
    assert!(sb.len() >= complete && sb.len() <= complete + 1);
    if let Some(b) = row0 {
        assert_eq!(sb.row(0), b);
    }
    if let Some(b) = row1 {
        assert_eq!(sb.row(1), b);
    }
    std::mem::forget(schema);
}

#[cfg(feature = "kani-heavy")]
#[kani::proof]
#[kani::unwind(15)]
#[kani::stub(std::str::from_utf8, utf8_by_table)]
fn projection_skips_the_unread_fields_like_reading_them() {
    // nine minutes and 16 GB at one row of 14 bytes (the skipped double is 8 of them): `kani-heavy`
    projection::<14, 1>();
}

/// 8. An enum of three symbols and a `fixed(4)`, `{e: enum, f: fixed(4)}`, `ROWS` rows, blocks of at most `N`
/// bytes: an index outside `0..3` is an error, inside it the exact byte-wide index; the fixed rows are the
/// reference's 4 bytes.
fn enum_and_fixed<const N: usize, const ROWS: usize>() {
    fn row<'a>(dec: &mut Decoder<'a>) -> Option<(u8, &'a [u8])> {
        // an enum index is an int in `0..symbol count`, independently of `read_enum`
        let index = match dec.read_int() {
            Ok(v) if (0..3).contains(&v) => Some(v as u8),
            _ => None,
        };
        match (index, dec.read_raw(4, "")) {
            (Some(e), Ok(f)) => Some((e, f)),
            _ => None,
        }
    }
    let buf: [u8; N] = kani::any();
    let data = any_slice(&buf);
    let mut reference = Decoder::new(data);
    let row0 = row(&mut reference);
    let row1 = second_row(ROWS, &row0, || row(&mut reference));
    let (ok, complete) = outcome(ROWS, &row0, &row1);

    // the symbols play no part in decoding (the count does): three empty names allocate nothing
    let mut symbols = pinned([String::new(), String::new(), String::new()]);
    let mut fields = pinned([field(id(0)), field(id(1))]);
    let mut arena = pinned([
        leaf(SchemaKind::Enum { name: Name::simple(""), symbols: leaked(&mut symbols), default: None }),
        leaf(SchemaKind::Fixed { name: Name::simple(""), size: 4 }),
        record(leaked(&mut fields)),
    ]);
    let mut sizes = pinned([1, 4, 5]);
    let schema = Schema::unchecked(leaked(&mut arena), id(2), leaked(&mut sizes));
    let mut nodes = pinned([
        Node::Enum { count: 3, values: EnumValues::U8(Prim::with_capacity(ROOM)) },
        Node::Fixed { size: 4, values: Strings::with_capacity(ROOM, BYTES) },
    ]);
    let (decoded, position) = decode(data, &schema, &mut *nodes, &[Some(0), Some(1)], ROWS);

    assert_eq!(decoded, ok);
    let Node::Enum { values: EnumValues::U8(pe), .. } = &nodes[0] else { panic!() };
    let (ve, ne) = pe.buffers();
    let sf = strings(&nodes[1]);
    assert!(ne.is_empty() && sf.nulls().is_empty());
    if ok {
        assert_eq!(position, reference.position());
    }
    assert!(ve.len() <= complete + 1 && sf.len() <= ve.len() && sf.len() >= complete);
    if let Some((e, f)) = row0 {
        assert!(ve[0] == e && sf.row(0) == f);
    }
    if let Some((e, f)) = row1 {
        assert!(ve[1] == e && sf.row(1) == f);
    }
    std::mem::forget(schema);
}

#[kani::proof]
#[kani::unwind(12)]
fn enum_index_is_range_checked_and_fixed_bytes_are_exact_one_row() {
    enum_and_fixed::<10, 1>();
}

#[cfg(feature = "kani-heavy")]
#[kani::proof]
#[kani::unwind(12)]
fn enum_index_is_range_checked_and_fixed_bytes_are_exact() {
    enum_and_fixed::<16, 2>();
}

/// 9. The row-count contract: a block of at most `N` bytes claiming `ROWS` rows of `{a: int, c: boolean}` decodes
/// without a panic; it succeeds exactly when the bytes hold every claimed row, and otherwise fails at the first row
/// the bytes do not hold, with the rows before it complete in the buffers and nothing past the claimed count.
fn claimed_rows<const N: usize, const ROWS: usize>() {
    fn row(dec: &mut Decoder<'_>) -> Option<(i32, bool)> {
        match (dec.read_int(), dec.read_boolean()) {
            (Ok(a), Ok(c)) => Some((a, c)),
            _ => None,
        }
    }
    let buf: [u8; N] = kani::any();
    let data = any_slice(&buf);
    let mut reference = Decoder::new(data);
    let row0 = row(&mut reference);
    let row1 = if row0.is_some() { row(&mut reference) } else { None };
    let row2 = if ROWS > 2 && row1.is_some() { row(&mut reference) } else { None };
    let complete = usize::from(row0.is_some()) + usize::from(row1.is_some()) + usize::from(row2.is_some());
    let ok = complete == ROWS;

    let mut fields = pinned([field(id(0)), field(id(1))]);
    let mut arena = pinned([leaf(SchemaKind::Int), leaf(SchemaKind::Boolean), record(leaked(&mut fields))]);
    let mut sizes = pinned([1, 1, 2]);
    let schema = Schema::unchecked(leaked(&mut arena), id(2), leaked(&mut sizes));
    let mut nodes = pinned([int(), boolean()]);
    let (decoded, position) = decode(data, &schema, &mut *nodes, &[Some(0), Some(1)], ROWS);

    assert_eq!(decoded, ok);
    assert!(position <= data.len());
    let (va, na) = ints(&nodes[0]);
    let (vc, nc) = booleans(&nodes[1]);
    assert!(na.is_empty() && nc.is_empty());
    if ok {
        assert_eq!(position, reference.position());
    } else {
        assert!(complete < ROWS);
    }
    assert_eq!(vc.len(), complete);
    assert!(va.len() == complete || (!ok && va.len() == complete + 1));
    assert!(va.len() <= ROWS);
    if let Some((a, c)) = row0 {
        assert!(va[0] == a && vc[0] == u8::from(c));
    }
    if let Some((a, c)) = row1 {
        assert!(va[1] == a && vc[1] == u8::from(c));
    }
    if let Some((a, c)) = row2 {
        assert!(va[2] == a && vc[2] == u8::from(c));
    }
    std::mem::forget(schema);
}

#[kani::proof]
#[kani::unwind(12)]
fn two_claimed_rows_beyond_the_bytes_fail_at_the_right_row() {
    claimed_rows::<5, 2>();
}

#[cfg(feature = "kani-heavy")]
#[kani::proof]
#[kani::unwind(12)]
fn claimed_rows_beyond_the_bytes_fail_at_the_right_row() {
    claimed_rows::<8, 3>();
}
