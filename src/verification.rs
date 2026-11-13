//! Kani proof harnesses (`cargo kani -p duckdb-avro`, `make kani`) over the extension's pure value conversions and
//! schema rules: the reader's decimal, time, timestamp, duration and UUID logic (`reader::node`), its union member
//! names, `decimal` attribute rule and storage widths (`reader::schema`), the writer's decimal sizing and UUID layout
//! (`copy::schema`, `copy::encode`) and its identifier check. Nothing here touches the engine: the helpers take and
//! return plain integers, byte arrays and short strings, so the harnesses run without a DuckDB build. (The sanitizer
//! and the union layout build growable strings and vectors, which put them beyond CBMC at any useful bound.)
//!
//! Each harness states its input bound; CBMC explores every value within it (all i32 / i64 / i128 / byte patterns
//! where the type is fixed-width, every length up to the bound where a slice is involved). A harness that drops a
//! `Result<_, Error>` of symbolic variant carries `#[kani::unwind(2)]`: `Error::extra_info` is a `Vec`, whose drop
//! glue loops over a length that is unconstrained in the `Ok` branch, and CBMC would unwind that loop forever.

use avro_core::Encoder;
use duckdb_ext_api::sys::duckdb_v2_hugeint_t;

use duckdb_ext_api::TypeId;
use duckdb_ext_api::sys::DUCKDB_V2_LOGICAL_TYPE_ID;
use duckdb_ext_api::types::type_id;

use crate::copy::{
    Plan, UUID_FIXED_SIZE, adjust_to_utc, avro_type_name, decimal_internal_type_id, hugeint_to_i128, is_avro_id,
    is_named_schema, is_temporal, min_bytes_required_for_decimal, primitive_plan, temporal_logical_type_name,
    union_branch_index, uuid_bytes, write_decimal,
};
use crate::reader::{
    Annotated, LeafError, LeafKind, LeafType, Shape, UnionLayout, decimal_annotation, decimal_width,
    duration_to_interval, enum_width, leaf, parse_annotation, parse_uuid, read_decimal, time_micros_checked,
    time_millis_to_micros, timestamp_millis_to_micros, union_layout, union_member_name, uuid_from_bytes,
};

/// Well-formed UTF-8 by the byte-sequence table of the Unicode standard (Table 3-7), independently of `core::str`.
pub(crate) fn well_formed_utf8(bytes: &[u8]) -> bool {
    let mut i = 0;
    while i < bytes.len() {
        let (len, lo, hi) = match bytes[i] {
            0x00..=0x7f => (1, 0x80, 0xbf),
            0xc2..=0xdf => (2, 0x80, 0xbf),
            0xe0 => (3, 0xa0, 0xbf),
            0xe1..=0xec | 0xee..=0xef => (3, 0x80, 0xbf),
            0xed => (3, 0x80, 0x9f),
            0xf0 => (4, 0x90, 0xbf),
            0xf1..=0xf3 => (4, 0x80, 0xbf),
            0xf4 => (4, 0x80, 0x8f),
            _ => return false,
        };
        if i + len > bytes.len() || (len > 1 && !(lo..=hi).contains(&bytes[i + 1])) {
            return false;
        }
        let mut k = 2;
        while k < len {
            if !(0x80..=0xbf).contains(&bytes[i + k]) {
                return false;
            }
            k += 1;
        }
        i += len;
    }
    true
}

/// Every string of at most `N` bytes: a symbolic length of `buf` assumed well-formed UTF-8 (building the string from
/// symbolic chars or validating it with `str::from_utf8` costs CBMC orders of magnitude more).
fn any_str<const N: usize>(buf: &[u8; N]) -> &str {
    let len: usize = kani::any();
    kani::assume(len <= N);
    let bytes = &buf[..len];
    kani::assume(well_formed_utf8(bytes));
    // SAFETY: `well_formed_utf8` is the UTF-8 definition, assumed to hold for `bytes` just above
    unsafe { std::str::from_utf8_unchecked(bytes) }
}

/// `10^p` for `p` in `0..=38` (an i128 holds 10^38), computed by the compiler so that CBMC sees a table lookup and
/// not 38 symbolic 128-bit multiplications.
const POW10: [i128; 39] = {
    let mut table = [1i128; 39];
    let mut i = 1;
    while i < 39 {
        table[i] = table[i - 1] * 10;
        i += 1;
    }
    table
};

/// The mathematical value of up to 16 big-endian two's complement bytes: sign-extended to 16 bytes and read as i128.
fn reference_decimal(raw: &[u8]) -> i128 {
    let fill = if raw.first().is_some_and(|b| b & 0x80 != 0) { 0xff } else { 0x00 };
    let mut padded = [fill; 16];
    padded[16 - raw.len()..].copy_from_slice(raw);
    i128::from_be_bytes(padded)
}

/// `read_decimal` of any 0..=16 bytes is the sign-extended two's complement value, and each storage width the reader
/// pushes (`as i16` / `as i32` / `as i64` / i128) keeps exactly the low bytes of it: sign extension for inputs
/// shorter than the width, the trailing bytes for inputs longer than it (what the C++ reader's shifts produce).
#[kani::proof]
#[kani::unwind(17)]
fn read_decimal_is_sign_extended_two_complement_at_every_width() {
    let buf: [u8; 16] = kani::any();
    let len: usize = kani::any();
    kani::assume(len <= 16);
    let raw = &buf[..len];
    let value = read_decimal(raw);
    let expected = reference_decimal(raw);
    assert_eq!(value, expected);
    assert_eq!(value as i16, expected as i16);
    assert_eq!(value as i32, expected as i32);
    assert_eq!(value as i64, expected as i64);
    if len >= 2 {
        assert_eq!(value as i16, i16::from_be_bytes([raw[len - 2], raw[len - 1]]));
    }
    if len >= 4 {
        assert_eq!(value as i32, i32::from_be_bytes(raw[len - 4..].try_into().unwrap()));
    }
    if len >= 8 {
        assert_eq!(value as i64, i64::from_be_bytes(raw[len - 8..].try_into().unwrap()));
    }
}

/// The writer's `min_bytes_required_for_decimal(p)` for every precision 1..=38 is the byte count of the two's
/// complement representation of `10^p - 1` (its magnitude bits plus a sign bit, rounded up to whole bytes), computed
/// here independently with u128 arithmetic; and that many bytes hold both `10^p - 1` and `-(10^p - 1)`.
#[kani::proof]
fn min_bytes_required_for_decimal_matches_two_complement_width() {
    let precision: u8 = kani::any();
    kani::assume((1..=38).contains(&precision));
    let max = POW10[usize::from(precision)] as u128 - 1;
    let signed_bits = (128 - max.leading_zeros()) + 1;
    let size = min_bytes_required_for_decimal(precision);
    assert_eq!(size, signed_bits.div_ceil(8) as usize);
    assert!((1..=16).contains(&size));
    // `size` bytes hold `-bound..bound` in two's complement (the bound in u128: for 16 bytes it is 2^127)
    let bound = 1u128 << (8 * size as u32 - 1);
    assert!(max < bound);
}

/// Every unscaled value a DECIMAL(p, s) can hold (`|value| < 10^p`, p in 1..=38) written by the writer as the `fixed`
/// of `min_bytes_required_for_decimal(p)` bytes is read back unchanged by the reader.
#[kani::proof]
#[kani::unwind(17)]
fn decimal_round_trips_through_the_writers_fixed() {
    let precision: u8 = kani::any();
    kani::assume((1..=38).contains(&precision));
    let unscaled: i128 = kani::any();
    let limit = POW10[usize::from(precision)];
    kani::assume(unscaled > -limit && unscaled < limit);
    let mut enc = Encoder::new();
    write_decimal(&mut enc, unscaled, precision);
    assert_eq!(enc.len(), min_bytes_required_for_decimal(precision));
    assert_eq!(read_decimal(enc.as_bytes()), unscaled);
}

/// `timestamp-millis` scaling for every i64: never panics, is an error exactly when `millis * 1000` leaves i64 (i.e.
/// outside `i64::MIN / 1000 ..= i64::MAX / 1000`), and is the exact product otherwise (checked against a 128-bit
/// product).
#[kani::proof]
#[kani::unwind(2)]
fn timestamp_millis_scaling_never_panics_and_is_exact() {
    let millis: i64 = kani::any();
    let fits = (i64::MIN / 1000..=i64::MAX / 1000).contains(&millis);
    match timestamp_millis_to_micros(millis) {
        Ok(micros) => {
            assert!(fits);
            assert_eq!(i128::from(micros), i128::from(millis) * 1000);
        }
        Err(_) => {
            assert!(!fits);
        }
    }
}

/// `time-millis` accepts exactly `0..=86_400_000` (every i32) and scales to the exact microseconds; `time-micros`
/// accepts exactly `0..=86_400_000_000` (every i64) unchanged.
#[kani::proof]
#[kani::unwind(2)]
fn time_millis_and_micros_accept_exactly_one_day() {
    let millis: i32 = kani::any();
    let result = time_millis_to_micros(millis);
    assert_eq!(result.is_ok(), (0..=86_400_000).contains(&millis));
    if let Ok(micros) = result {
        assert_eq!(micros, i64::from(millis) * 1000);
    }
    let micros: i64 = kani::any();
    assert_eq!(time_micros_checked(micros).ok(), (0..=86_400_000_000).contains(&micros).then_some(micros));
}

/// `duration` (fixed(12): months, days, milliseconds as little-endian u32) for every 12 bytes: never panics, an
/// error exactly when months or days exceed i32, months and days carried over exactly and the microseconds the exact
/// product (a u32 times 1000 fits an i64).
#[kani::proof]
#[kani::unwind(2)]
fn duration_to_interval_never_panics() {
    let raw: [u8; 12] = kani::any();
    let months = u32::from_le_bytes([raw[0], raw[1], raw[2], raw[3]]);
    let days = u32::from_le_bytes([raw[4], raw[5], raw[6], raw[7]]);
    let millis = u32::from_le_bytes([raw[8], raw[9], raw[10], raw[11]]);
    let result = duration_to_interval(&raw);
    assert_eq!(result.is_ok(), months <= i32::MAX as u32 && days <= i32::MAX as u32);
    if let Ok(interval) = result {
        assert_eq!(interval.months as u32, months);
        assert_eq!(interval.days as u32, days);
        assert_eq!(interval.micros, i64::from(millis) * 1000);
    }
}

/// The engine's UUID storage form (big-endian i128 with the high bit flipped) and the 16 Avro bytes convert back and
/// forth losslessly in both directions, for every byte pattern and every hugeint: the writer's `uuid_bytes` inverts
/// the reader's `uuid_from_bytes` and vice versa (so the high-bit flip is an involution), and the stored integers
/// order like the byte strings.
#[kani::proof]
fn uuid_storage_form_round_trips() {
    let bytes: [u8; 16] = kani::any();
    let stored = uuid_from_bytes(bytes);
    let hugeint = duckdb_v2_hugeint_t { lower: stored as u64, upper: (stored >> 64) as i64 };
    assert_eq!(hugeint_to_i128(hugeint), stored);
    assert_eq!(uuid_bytes(hugeint), bytes);
    let other: [u8; 16] = kani::any();
    assert_eq!(uuid_from_bytes(other) < stored, u128::from_be_bytes(other) < u128::from_be_bytes(bytes));
    let hugeint = duckdb_v2_hugeint_t { lower: kani::any(), upper: kani::any() };
    assert_eq!(uuid_from_bytes(uuid_bytes(hugeint)), hugeint_to_i128(hugeint));
}

/// The value of an ASCII hex digit (the caller checked `is_ascii_hexdigit`).
fn hex_value(c: u8) -> u8 {
    match c {
        b'0'..=b'9' => c - b'0',
        b'a'..=b'f' => c - b'a' + 10,
        _ => c - b'A' + 10,
    }
}

/// `parse_uuid` on any byte string of length 0..=40: never panics, accepts exactly the 36-byte `8-4-4-4-12` form of
/// hex digits in either case with `-` at offsets 8, 13, 18 and 23, and then yields the nibbles packed big-endian.
#[kani::proof]
#[kani::unwind(42)]
fn parse_uuid_accepts_exactly_the_hyphenated_hex_form() {
    let buf: [u8; 40] = kani::any();
    let len: usize = kani::any();
    kani::assume(len <= 40);
    let text = &buf[..len];
    let result = parse_uuid(text);
    let mut valid = len == 36;
    let mut expected = [0u8; 16];
    if valid {
        let mut nibbles = 0;
        for (i, &c) in text.iter().enumerate() {
            if matches!(i, 8 | 13 | 18 | 23) {
                valid &= c == b'-';
            } else {
                valid &= c.is_ascii_hexdigit();
                let digit = if c.is_ascii_hexdigit() { hex_value(c) } else { 0 };
                expected[nibbles / 2] |= if nibbles % 2 == 0 { digit << 4 } else { digit };
                nibbles += 1;
            }
        }
    }
    assert_eq!(result, valid.then_some(expected));
}

/// The identifier grammar `[A-Za-z_][A-Za-z0-9_]*` over bytes, written out: a non-empty string whose first byte is an
/// ASCII letter or `_` and whose other bytes are ASCII letters, digits or `_` (a non-ASCII byte is none of those).
fn reference_identifier(bytes: &[u8]) -> bool {
    let Some(&first) = bytes.first() else { return false };
    if !(first.is_ascii_alphabetic() || first == b'_') {
        return false;
    }
    bytes[1..].iter().all(|&b| b.is_ascii_alphanumeric() || b == b'_')
}

/// The writer's identifier check agrees with the grammar on every string of at most 8 bytes (the empty string is
/// rejected, a leading digit is rejected, non-ASCII is rejected).
#[kani::proof]
#[kani::unwind(10)]
fn is_avro_id_matches_the_grammar() {
    let buf: [u8; 8] = kani::any();
    let text = any_str(&buf);
    assert_eq!(is_avro_id(text), reference_identifier(text.as_bytes()));
}

/// Member names are `u<index>` for every index below 256 (the digits re-read as the index, no leading zero), and
/// every member index below 256 (the engine admits at most 255 members) fits the u8 union tag.
#[kani::proof]
#[kani::unwind(6)]
fn union_member_names_are_u_index() {
    let index: usize = kani::any();
    kani::assume(index < 256);
    let name = union_member_name(index);
    let bytes = name.as_bytes();
    assert_eq!(bytes[0], b'u');
    assert!(bytes.len() >= 2 && bytes.len() <= 4);
    let mut value = 0usize;
    for &digit in &bytes[1..] {
        assert!(digit.is_ascii_digit());
        value = value * 10 + usize::from(digit - b'0');
    }
    assert_eq!(value, index);
    assert!(bytes.len() == 2 || bytes[1] != b'0');
    assert_eq!(usize::from(index as u8), index);
}

/// The lenient `decimal` rule on every pair of optional i64 attributes: honoured exactly when a precision is present
/// in 1..=38 and the scale (0 when absent) lies in 0..=precision, and then carried over exactly; anything else
/// (string-typed attributes arrive as absent) falls back to the underlying type.
#[kani::proof]
fn decimal_annotation_matches_the_rule() {
    let precision: Option<i64> = kani::any();
    let scale: Option<i64> = kani::any();
    let result = decimal_annotation(precision, scale);
    match precision {
        Some(p) if (1..=38).contains(&p) && (0..=p).contains(&scale.unwrap_or(0)) => {
            assert_eq!(result, Some((p as u8, scale.unwrap_or(0) as u8)));
        }
        _ => assert_eq!(result, None),
    }
}

/// The storage widths: an ENUM of `count` symbols (every count up to u32::MAX, the engine's bound) stores every index
/// below `count` losslessly in `enum_width(count)` bytes; a DECIMAL of every precision 1..=38 stores every unscaled
/// value of that many digits in `decimal_width(precision)` bytes (two's complement).
#[kani::proof]
fn storage_widths_hold_every_value() {
    let count: usize = kani::any();
    kani::assume(count <= u32::MAX as usize);
    let index: usize = kani::any();
    kani::assume(index < count);
    let round_trip = match enum_width(count) {
        1 => usize::from(index as u8),
        2 => usize::from(index as u16),
        _ => index as u32 as usize,
    };
    assert_eq!(round_trip, index);
    let precision: u8 = kani::any();
    kani::assume((1..=38).contains(&precision));
    let width = decimal_width(precision);
    assert!(matches!(width, 2 | 4 | 8 | 16));
    let max = POW10[usize::from(precision)] - 1;
    let bound = if width == 16 { i128::MAX } else { (1i128 << (8 * width as u32 - 1)) - 1 };
    assert!(max <= bound);
}

/// `parse_annotation(name, ..)` for every `adjust-to-utc` and every pair of optional `precision` / `scale`
/// attributes is `expected(utc, decimal_annotation(precision, scale))` (the `decimal` rule is proven separately).
fn check_annotation(name: &str, expected: fn(bool, Option<(u8, u8)>) -> Option<Annotated>) {
    let utc: bool = kani::any();
    let precision: Option<i64> = kani::any();
    let scale: Option<i64> = kani::any();
    assert_eq!(parse_annotation(name, utc, precision, scale), expected(utc, decimal_annotation(precision, scale)));
}

/// `parse_annotation` on every logical type name the spec defines, for every `adjust-to-utc` and every pair of
/// optional attributes: each is its annotation (the `timestamp-*` ones carry `adjust-to-utc`, `decimal` its
/// validated attributes or nothing); an unknown name, another case, or the empty name is nothing. (The names are
/// concrete: a name chosen through a symbolic index makes every comparison a symbolic pointer dereference, which
/// CBMC does not finish in twenty minutes.)
#[kani::proof]
fn parse_annotation_knows_exactly_the_spec_names() {
    check_annotation("date", |_, _| Some(Annotated::Date));
    check_annotation("decimal", |_, decimal| decimal.map(|(precision, scale)| Annotated::Decimal { precision, scale }));
    check_annotation("time-millis", |_, _| Some(Annotated::TimeMillis));
    check_annotation("time-micros", |_, _| Some(Annotated::TimeMicros));
    check_annotation("timestamp-millis", |utc, _| Some(Annotated::TimestampMillis { utc }));
    check_annotation("timestamp-micros", |utc, _| Some(Annotated::TimestampMicros { utc }));
    check_annotation("timestamp-nanos", |utc, _| Some(Annotated::TimestampNanos { utc }));
    check_annotation("uuid", |_, _| Some(Annotated::Uuid));
    check_annotation("duration", |_, _| Some(Annotated::Duration));
    check_annotation("local-timestamp-millis", |_, _| Some(Annotated::LocalTimestampMillis));
    check_annotation("local-timestamp-micros", |_, _| Some(Annotated::LocalTimestampMicros));
    check_annotation("local-timestamp-nanos", |_, _| Some(Annotated::LocalTimestampNanos));
    check_annotation("char", |_, _| None);
    check_annotation("Date", |_, _| None);
    check_annotation("time-millis ", |_, _| None);
    check_annotation("", |_, _| None);
}

/// Every annotation, from a symbolic choice (every variant, every `utc`, every precision and scale).
fn any_annotation() -> Option<Annotated> {
    let choice: u8 = kani::any();
    kani::assume(choice <= 12);
    let utc: bool = kani::any();
    Some(match choice {
        0 => Annotated::Date,
        1 => Annotated::TimeMillis,
        2 => Annotated::TimeMicros,
        3 => Annotated::TimestampMillis { utc },
        4 => Annotated::TimestampMicros { utc },
        5 => Annotated::TimestampNanos { utc },
        6 => Annotated::LocalTimestampMillis,
        7 => Annotated::LocalTimestampMicros,
        8 => Annotated::LocalTimestampNanos,
        9 => Annotated::Decimal { precision: kani::any(), scale: kani::any() },
        10 => Annotated::Uuid,
        11 => Annotated::Duration,
        _ => return None,
    })
}

/// Every primitive Avro type (every size of a `fixed`).
fn any_leaf_kind() -> LeafKind {
    let choice: u8 = kani::any();
    kani::assume(choice < 8);
    match choice {
        0 => LeafKind::Boolean,
        1 => LeafKind::Int,
        2 => LeafKind::Long,
        3 => LeafKind::Float,
        4 => LeafKind::Double,
        5 => LeafKind::String,
        6 => LeafKind::Bytes,
        _ => LeafKind::Fixed(kani::any()),
    }
}

/// Whether two shapes of primitive values are the same (the nested shapes are not compared: `Shape` has no
/// `PartialEq`, its nested variants hold vectors).
fn same_leaf_shape(a: &Shape, b: &Shape) -> bool {
    match (a, b) {
        (Shape::Boolean, Shape::Boolean)
        | (Shape::Int, Shape::Int)
        | (Shape::Long, Shape::Long)
        | (Shape::TimeMillis, Shape::TimeMillis)
        | (Shape::TimeMicros, Shape::TimeMicros)
        | (Shape::TimestampMillis, Shape::TimestampMillis)
        | (Shape::Float, Shape::Float)
        | (Shape::Double, Shape::Double)
        | (Shape::Bytes, Shape::Bytes)
        | (Shape::String, Shape::String)
        | (Shape::Uuid, Shape::Uuid)
        | (Shape::UuidText, Shape::UuidText)
        | (Shape::Duration, Shape::Duration) => true,
        (Shape::Fixed(x), Shape::Fixed(y)) => x == y,
        (Shape::Decimal { fixed: f1, width: w1 }, Shape::Decimal { fixed: f2, width: w2 }) => f1 == f2 && w1 == w2,
        _ => false,
    }
}

/// The type-mapping spec for primitives (the module doc of `reader::schema`, the C++ extension's table), written by
/// logical type rather than by Avro type as the code is: without an annotation every primitive is its own type;
/// `date` and `time-millis` belong to `int`, `time-micros`, the `timestamp-*` and `local-timestamp-*` to `long`
/// (`adjust-to-utc` picks the WITH TIME ZONE type of the `timestamp-*`, the `local-*` ignore it), `uuid` to `string`
/// and to `fixed(16)` (another size is its own error), `decimal` to `bytes` and `fixed` (DECIMAL(p, s) stored in
/// `decimal_width(p)` bytes), `duration` to `fixed(12)` (another size its own error); every other combination of a
/// known logical type and an Avro type is a mismatch.
fn expected_leaf(kind: LeafKind, annotation: Option<Annotated>) -> std::result::Result<(LeafType, Shape), LeafError> {
    use LeafType::Id;
    let Some(annotation) = annotation else {
        return Ok(match kind {
            LeafKind::Boolean => (Id(type_id::BOOLEAN), Shape::Boolean),
            LeafKind::Int => (Id(type_id::INTEGER), Shape::Int),
            LeafKind::Long => (Id(type_id::BIGINT), Shape::Long),
            LeafKind::Float => (Id(type_id::FLOAT), Shape::Float),
            LeafKind::Double => (Id(type_id::DOUBLE), Shape::Double),
            LeafKind::String => (Id(type_id::VARCHAR), Shape::String),
            LeafKind::Bytes => (Id(type_id::BLOB), Shape::Bytes),
            LeafKind::Fixed(size) => (Id(type_id::BLOB), Shape::Fixed(size)),
        });
    };
    let on_long = |type_: TypeId, shape: Shape| {
        if kind == LeafKind::Long { Ok((Id(type_), shape)) } else { Err(LeafError::Mismatch) }
    };
    let with_zone = |utc: bool| if utc { type_id::TIMESTAMP_TZ } else { type_id::TIMESTAMP };
    match annotation {
        Annotated::Date if kind == LeafKind::Int => Ok((Id(type_id::DATE), Shape::Int)),
        Annotated::TimeMillis if kind == LeafKind::Int => Ok((Id(type_id::TIME), Shape::TimeMillis)),
        Annotated::Date | Annotated::TimeMillis => Err(LeafError::Mismatch),
        Annotated::TimeMicros => on_long(type_id::TIME, Shape::TimeMicros),
        Annotated::TimestampMillis { utc } => on_long(with_zone(utc), Shape::TimestampMillis),
        Annotated::TimestampMicros { utc } => on_long(with_zone(utc), Shape::Long),
        Annotated::TimestampNanos { utc } => {
            on_long(if utc { type_id::TIMESTAMP_TZ_NS } else { type_id::TIMESTAMP_NS }, Shape::Long)
        }
        Annotated::LocalTimestampMillis => on_long(type_id::TIMESTAMP, Shape::TimestampMillis),
        Annotated::LocalTimestampMicros => on_long(type_id::TIMESTAMP, Shape::Long),
        Annotated::LocalTimestampNanos => on_long(type_id::TIMESTAMP_NS, Shape::Long),
        Annotated::Uuid => match kind {
            LeafKind::String => Ok((Id(type_id::UUID), Shape::UuidText)),
            LeafKind::Fixed(16) => Ok((Id(type_id::UUID), Shape::Uuid)),
            LeafKind::Fixed(_) => Err(LeafError::UuidSize),
            _ => Err(LeafError::Mismatch),
        },
        Annotated::Decimal { precision, scale } => {
            let fixed = match kind {
                LeafKind::Bytes => None,
                LeafKind::Fixed(size) => Some(size),
                _ => return Err(LeafError::Mismatch),
            };
            Ok((LeafType::Decimal { precision, scale }, Shape::Decimal { fixed, width: decimal_width(precision) }))
        }
        Annotated::Duration => match kind {
            LeafKind::Fixed(12) => Ok((Id(type_id::INTERVAL), Shape::Duration)),
            LeafKind::Fixed(_) => Err(LeafError::DurationSize),
            _ => Err(LeafError::Mismatch),
        },
    }
}

/// The reader's type mapping `leaf` on every primitive Avro type (every `fixed` size) and every annotation (every
/// variant, `adjust-to-utc`, precision and scale) agrees with the spec table `expected_leaf`: the DuckDB type, the
/// decoding shape, and the class of error. (The shapes are forgotten rather than dropped: `Shape`'s drop glue
/// recurses through its nested variants, which CBMC would unwind.)
#[kani::proof]
fn reader_type_mapping_matches_the_spec_table() {
    let kind = any_leaf_kind();
    let annotation = any_annotation();
    let result = leaf(kind, annotation);
    let expected = expected_leaf(kind, annotation);
    match (result, expected) {
        (Ok((type_, shape)), Ok((expected_type, expected_shape))) => {
            assert_eq!(type_, expected_type);
            assert!(same_leaf_shape(&shape, &expected_shape));
            std::mem::forget(shape);
            std::mem::forget(expected_shape);
        }
        (Err(error), Err(expected_error)) => assert_eq!(error, expected_error),
        (Ok((_, shape)), Err(_)) => {
            std::mem::forget(shape);
            panic!("leaf accepted what the table refuses");
        }
        (Err(_), Ok((_, expected_shape))) => {
            std::mem::forget(expected_shape);
            panic!("leaf refused what the table accepts");
        }
    }
}

/// Every type id (the C enum is an i32 newtype).
fn any_type_id() -> TypeId {
    DUCKDB_V2_LOGICAL_TYPE_ID(kani::any())
}

/// The writer's table (the module doc of `copy::schema`): the DuckDB types the writer accepts and the Avro type of
/// each; every other type id is refused.
const WRITER_TABLE: [(TypeId, &str); 22] = [
    (type_id::VARCHAR, "string"),
    (type_id::BLOB, "bytes"),
    (type_id::INTEGER, "int"),
    (type_id::BIGINT, "long"),
    (type_id::FLOAT, "float"),
    (type_id::DOUBLE, "double"),
    (type_id::BOOLEAN, "boolean"),
    (type_id::SQLNULL, "null"),
    (type_id::DATE, "int"),
    (type_id::TIME, "long"),
    (type_id::TIMESTAMP, "long"),
    (type_id::TIMESTAMP_MS, "long"),
    (type_id::TIMESTAMP_TZ, "long"),
    (type_id::TIMESTAMP_NS, "long"),
    (type_id::TIMESTAMP_TZ_NS, "long"),
    (type_id::UUID, "fixed"),
    (type_id::DECIMAL, "fixed"),
    (type_id::STRUCT, "record"),
    (type_id::LIST, "array"),
    (type_id::MAP, "array"),
    (type_id::ENUM, "enum"),
    (type_id::UNION, "union"),
];

/// The Avro type the encoder's plan of a primitive writes, by the plan's variant (`None` for no plan).
fn plan_avro_type(plan: &Option<Plan>) -> Option<&'static str> {
    Some(match plan.as_ref()? {
        Plan::Null => "null",
        Plan::Boolean => "boolean",
        Plan::Int32 => "int",
        Plan::Int64 => "long",
        Plan::Float => "float",
        Plan::Double => "double",
        Plan::String => "string",
        Plan::Bytes => "bytes",
        Plan::Uuid => "fixed",
        _ => "nested",
    })
}

/// The writer's type tables: every row of the module's table concretely, then every other type id (every i32)
/// symbolically. Per row, `avro_type_name` is the table's Avro type; the temporal logical types are `date` on DATE,
/// `time-micros` on TIME, `timestamp-millis` / `-micros` / `-nanos` on the TIMESTAMP flavours by their unit,
/// `adjust-to-utc` on exactly the WITH TIME ZONE ones, and a temporal type is an `int` (DATE) or a `long`; the named
/// Avro types are exactly `record`, `enum` and `fixed`; and the encoder's plan of the id writes the Avro type the
/// schema declares (DECIMAL and the nested types take their plans from the type's parameters: none from the id
/// alone). Every id outside the table is refused by all of them. (Rows concrete, see
/// `parse_annotation_knows_exactly_the_spec_names`.)
#[kani::proof]
#[kani::unwind(23)]
fn writer_type_tables_match_the_module_table() {
    let mut i = 0;
    while i < WRITER_TABLE.len() {
        let (id, avro) = WRITER_TABLE[i];
        assert_eq!(avro_type_name(id), Some(avro));
        let temporal = match id {
            type_id::DATE => Some("date"),
            type_id::TIME => Some("time-micros"),
            type_id::TIMESTAMP_MS => Some("timestamp-millis"),
            type_id::TIMESTAMP | type_id::TIMESTAMP_TZ => Some("timestamp-micros"),
            type_id::TIMESTAMP_NS | type_id::TIMESTAMP_TZ_NS => Some("timestamp-nanos"),
            _ => None,
        };
        assert_eq!(temporal_logical_type_name(id), temporal);
        assert_eq!(is_temporal(id), temporal.is_some());
        if is_temporal(id) {
            assert_eq!(avro, if id == type_id::DATE { "int" } else { "long" });
        }
        assert_eq!(adjust_to_utc(id), id == type_id::TIMESTAMP_TZ || id == type_id::TIMESTAMP_TZ_NS);
        assert_eq!(is_named_schema(id), matches!(avro, "record" | "enum" | "fixed"));
        let plan = primitive_plan(id);
        let parameterized = matches!(
            id,
            type_id::DECIMAL | type_id::STRUCT | type_id::LIST | type_id::MAP | type_id::ENUM | type_id::UNION
        );
        assert_eq!(plan_avro_type(&plan), if parameterized { None } else { Some(avro) });
        std::mem::forget(plan);
        i += 1;
    }
    let id = any_type_id();
    let mut in_table = false;
    let mut i = 0;
    while i < WRITER_TABLE.len() {
        in_table |= WRITER_TABLE[i].0 == id;
        i += 1;
    }
    kani::assume(!in_table);
    assert!(avro_type_name(id).is_none());
    assert!(temporal_logical_type_name(id).is_none());
    assert!(!is_temporal(id) && !adjust_to_utc(id) && !is_named_schema(id));
    let plan = primitive_plan(id);
    assert!(plan.is_none());
    std::mem::forget(plan);
}

/// The primitive DuckDB types the writer accepts: the table without `null` and the nested types.
const PRIMITIVE_IDS: [TypeId; 16] = [
    type_id::BOOLEAN,
    type_id::INTEGER,
    type_id::BIGINT,
    type_id::FLOAT,
    type_id::DOUBLE,
    type_id::VARCHAR,
    type_id::BLOB,
    type_id::DATE,
    type_id::TIME,
    type_id::TIMESTAMP,
    type_id::TIMESTAMP_MS,
    type_id::TIMESTAMP_TZ,
    type_id::TIMESTAMP_NS,
    type_id::TIMESTAMP_TZ_NS,
    type_id::UUID,
    type_id::DECIMAL,
];

/// Every primitive DuckDB type the writer accepts comes back from the reader as the same type, through the two pure
/// halves: the writer's tables give the Avro type, the logical type, `adjust-to-utc` and the `fixed` size (UUID 16,
/// DECIMAL by `min_bytes_required_for_decimal`), the reader's `parse_annotation` and `leaf` map them back. The one
/// exception is TIMESTAMP_MS, read as TIMESTAMP (the reader scales `timestamp-millis` to microseconds, as the C++
/// does). DECIMAL for every precision 1..=38 and scale, with the reader's storage width; the reader's shape decodes
/// what the writer's plan encodes (an `int` as `Int`, a `long` as `Long` / `TimeMicros` / `TimestampMillis`).
#[kani::proof]
#[kani::unwind(17)]
fn writer_schema_reads_back_as_the_written_type() {
    let precision: u8 = kani::any();
    let scale: u8 = kani::any();
    kani::assume((1..=38).contains(&precision) && scale <= precision);
    let mut i = 0;
    while i < PRIMITIVE_IDS.len() {
        let id = PRIMITIVE_IDS[i];
        let (kind, logical_type) = match avro_type_name(id) {
            Some("boolean") => (LeafKind::Boolean, None),
            Some("int") => (LeafKind::Int, temporal_logical_type_name(id)),
            Some("long") => (LeafKind::Long, temporal_logical_type_name(id)),
            Some("float") => (LeafKind::Float, None),
            Some("double") => (LeafKind::Double, None),
            Some("string") => (LeafKind::String, None),
            Some("bytes") => (LeafKind::Bytes, None),
            Some("fixed") if id == type_id::UUID => (LeafKind::Fixed(UUID_FIXED_SIZE), Some("uuid")),
            Some("fixed") => (LeafKind::Fixed(min_bytes_required_for_decimal(precision)), Some("decimal")),
            _ => panic!("not a primitive"),
        };
        let attributes = if id == type_id::DECIMAL { Some((i64::from(precision), i64::from(scale))) } else { None };
        let annotation = logical_type.and_then(|name| {
            parse_annotation(name, adjust_to_utc(id), attributes.map(|a| a.0), attributes.map(|a| a.1))
        });
        let Ok((type_, shape)) = leaf(kind, annotation) else { panic!("the reader refuses what the writer writes") };
        let expected_type = match id {
            type_id::TIMESTAMP_MS => LeafType::Id(type_id::TIMESTAMP),
            type_id::DECIMAL => LeafType::Decimal { precision, scale },
            _ => LeafType::Id(id),
        };
        assert_eq!(type_, expected_type);
        let expected_shape = match id {
            type_id::BOOLEAN => Shape::Boolean,
            type_id::INTEGER | type_id::DATE => Shape::Int,
            type_id::BIGINT
            | type_id::TIMESTAMP
            | type_id::TIMESTAMP_TZ
            | type_id::TIMESTAMP_NS
            | type_id::TIMESTAMP_TZ_NS => Shape::Long,
            type_id::TIME => Shape::TimeMicros,
            type_id::TIMESTAMP_MS => Shape::TimestampMillis,
            type_id::FLOAT => Shape::Float,
            type_id::DOUBLE => Shape::Double,
            type_id::VARCHAR => Shape::String,
            type_id::BLOB => Shape::Bytes,
            type_id::UUID => Shape::Uuid,
            _ => Shape::Decimal {
                fixed: Some(min_bytes_required_for_decimal(precision)),
                width: decimal_width(precision),
            },
        };
        assert!(same_leaf_shape(&shape, &expected_shape));
        std::mem::forget(shape);
        std::mem::forget(expected_shape);
        i += 1;
    }
}

/// The writer's branches for `member_count` members are `[null, m0, m1, ...]` when the field is nullable and
/// `[m0, m1, ...]` otherwise; the reader's layout of them: the null branch (if any) has no member, the member it
/// reads branch `union_branch_index(tag, nullable)` into is `tag` for every tag, and the layout is Null / Nullable /
/// Union by the member count (one member reads back as that member's type, nullable).
fn check_union_inverse<const N: usize>(member_count: usize, nullable: bool, tag: u8) {
    let mut null_branch = [false; N];
    null_branch[0] = nullable;
    let branches = &null_branch[..member_count + usize::from(nullable)];
    let (branch_members, layout) = union_layout(branches);
    assert_eq!(branch_members.len(), branches.len());
    if nullable {
        assert_eq!(branch_members[0], None);
    }
    if usize::from(tag) < member_count {
        let branch = union_branch_index(tag, nullable);
        assert!(branch < branches.len());
        assert_eq!(branch_members[branch], Some(usize::from(tag)));
    }
    let expected = match member_count {
        0 => UnionLayout::Null,
        1 => UnionLayout::Nullable { branch: usize::from(nullable) },
        n => UnionLayout::Union { member_count: n },
    };
    assert_eq!(layout, expected);
}

/// `check_union_inverse` for every member count up to `N - 1`, both nullabilities and every tag. (The counts are
/// concrete: `union_layout` allocates a vector of the branch count, and a symbolic count costs CBMC minutes and
/// tens of GB at any bound.)
fn union_inverse_up_to<const N: usize>() {
    let tag: u8 = kani::any();
    let mut member_count = 0;
    while member_count < N {
        check_union_inverse::<N>(member_count, false, tag);
        check_union_inverse::<N>(member_count, true, tag);
        member_count += 1;
    }
}

/// The writer's union branch index and the reader's union layout invert each other for every member count up to 8
/// and every tag. (16 members take CBMC a minute, the engine's bound of 255 is out of reach: the unrolled work grows
/// with the square of the bound.)
#[kani::proof]
#[kani::unwind(10)]
fn union_branch_index_inverts_through_the_readers_layout() {
    union_inverse_up_to::<9>();
}

/// `union_layout` on every pattern of up to 8 null / non-null branches (every length concretely, the pattern
/// symbolic): a null branch has no member, a non-null one the number of non-null branches before it (so the members
/// are numbered in branch order, densely from 0), and the layout is Null for no member, Nullable naming the one
/// non-null branch for one, Union with the count otherwise.
#[kani::proof]
#[kani::unwind(10)]
fn union_layout_numbers_the_non_null_branches_in_order() {
    let pattern: [bool; 8] = kani::any();
    let mut len = 0;
    while len <= 8 {
        let null_branch = &pattern[..len];
        let (branch_members, layout) = union_layout(null_branch);
        assert_eq!(branch_members.len(), len);
        let mut members = 0;
        let mut first = None;
        let mut b = 0;
        while b < len {
            if null_branch[b] {
                assert_eq!(branch_members[b], None);
            } else {
                assert_eq!(branch_members[b], Some(members));
                members += 1;
                if first.is_none() {
                    first = Some(b);
                }
            }
            b += 1;
        }
        let expected = match (members, first) {
            (0, _) => UnionLayout::Null,
            (1, Some(branch)) => UnionLayout::Nullable { branch },
            (member_count, _) => UnionLayout::Union { member_count },
        };
        assert_eq!(layout, expected);
        len += 1;
    }
}

/// The writer's storage type of a DECIMAL (`decimal_internal_type_id`, the C API's rule) and the reader's storage
/// width (`decimal_width`) agree for every width: SMALLINT / INTEGER / BIGINT / HUGEINT are 2 / 4 / 8 / 16 bytes,
/// and nothing else is ever selected.
#[kani::proof]
fn decimal_storage_types_agree_between_writer_and_reader() {
    let width: u8 = kani::any();
    let bytes = match decimal_internal_type_id(width) {
        type_id::SMALLINT => 2,
        type_id::INTEGER => 4,
        type_id::BIGINT => 8,
        type_id::HUGEINT => 16,
        _ => 0,
    };
    assert_eq!(bytes, decimal_width(width));
}

/// `String::from_utf8_lossy`, what `avro_metadata` renders a non-text value with, on every byte string of at most
/// 4 bytes (every UTF-8 sequence length): never panics, and well-formed UTF-8 comes through unchanged. `core`'s
/// validation of symbolic bytes is slow for CBMC (four minutes and 15 GB here, two minutes and 8 GB for 3 bytes):
/// behind the `kani-heavy` feature, which `make kani` enables and the CI job does not.
#[cfg(feature = "kani-heavy")]
#[kani::proof]
#[kani::unwind(6)]
fn metadata_value_rendering_keeps_well_formed_utf8() {
    let buf: [u8; 4] = kani::any();
    let len: usize = kani::any();
    kani::assume(len <= 4);
    let bytes = &buf[..len];
    let rendered = String::from_utf8_lossy(bytes);
    if well_formed_utf8(bytes) {
        assert_eq!(rendered.as_bytes(), bytes);
    }
}
