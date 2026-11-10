//! Kani proof harnesses (`cargo kani -p duckdb-avro`, `make kani`) over the extension's pure value conversions: the
//! reader's decimal, time, timestamp, duration and UUID logic (`reader::node`) and the writer's decimal sizing and
//! UUID layout (`copy::schema`, `copy::encode`). Nothing here touches the engine: the helpers take and return plain
//! integers and byte arrays, so the harnesses run without a DuckDB build.
//!
//! Each harness states its input bound; CBMC explores every value within it (all i32 / i64 / i128 / byte patterns
//! where the type is fixed-width, every length up to the bound where a slice is involved). A harness that drops a
//! `Result<_, Error>` of symbolic variant carries `#[kani::unwind(2)]`: `Error::extra_info` is a `Vec`, whose drop
//! glue loops over a length that is unconstrained in the `Ok` branch, and CBMC would unwind that loop forever.

use avro_core::Encoder;
use duckdb_ext_api::sys::duckdb_v2_hugeint_t;

use crate::copy::{hugeint_to_i128, min_bytes_required_for_decimal, uuid_bytes, write_decimal};
use crate::reader::{
    duration_to_interval, parse_uuid, read_decimal, time_micros_checked, time_millis_to_micros,
    timestamp_millis_to_micros, uuid_from_bytes,
};

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
        Err(_) => assert!(!fits),
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
