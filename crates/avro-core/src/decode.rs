//! The Avro binary decoder: a pull API over a slice of decoded block bytes.
//!
//! The schema-driven consumer calls the typed `read_*` methods in schema order; nothing is materialized. Every read
//! is bounds-checked against the slice, so a truncated or corrupt block yields an error instead of a panic.

use crate::error::{Error, Result, invalid_input};
use crate::schema::{Schema, SchemaId, SchemaKind};

/// Recursion depth after which [`Decoder::skip`] gives up (a recursive schema whose values occupy no bytes).
const MAX_DEPTH: usize = 1000;

/// The error text for undecodable UTF-8 in a string or map key (the DuckDB layer surfaces it unchanged).
pub const INVALID_UNICODE: &str = "Avro file contains invalid unicode string";

/// Items per array or map block a decoder accepts when the items encode to zero bytes (`null`, an empty record, a
/// `fixed` of size 0: [`Schema::min_encoded_size`] is 0). Such a block has no payload to bound its count by, so a
/// corrupt or malicious count (2^32 nulls in a 178-byte file) would otherwise keep the decoder spinning; 16 Mi
/// zero-size items per block is far beyond what a writer produces.
pub const MAX_ZERO_SIZE_BLOCK_ITEMS: usize = 1 << 24;

/// Decodes zigzag varint `value` as the Avro spec defines it.
#[inline]
pub fn zigzag_decode(value: u64) -> i64 {
    ((value >> 1) as i64) ^ -((value & 1) as i64)
}

/// Header of an array or map block as [`Decoder::read_block_count`] returns it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockCount {
    /// Number of items in the block; `0` ends the array or map.
    pub count: usize,
    /// Size in bytes of the block's items when the writer recorded it (a negative count in the encoding).
    pub byte_size: Option<usize>,
}

/// A pull decoder over the bytes of one decompressed block (or any Avro binary buffer).
#[derive(Debug, Clone)]
pub struct Decoder<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Decoder<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Decoder { data, pos: 0 }
    }

    /// Current byte offset.
    #[inline]
    pub fn position(&self) -> usize {
        self.pos
    }

    /// Bytes not consumed yet.
    #[inline]
    pub fn remaining(&self) -> usize {
        self.data.len() - self.pos
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.pos >= self.data.len()
    }

    /// The unread tail.
    pub fn rest(&self) -> &'a [u8] {
        &self.data[self.pos..]
    }

    /// Moves to an absolute offset (clamped to the end).
    pub fn seek(&mut self, pos: usize) {
        self.pos = pos.min(self.data.len());
    }

    fn eof(&self, what: &str) -> Error {
        invalid_input!("Avro block truncated while reading {what} at offset {}", self.pos)
    }

    /// Takes `n` raw bytes.
    #[inline]
    pub fn read_raw(&mut self, n: usize, what: &str) -> Result<&'a [u8]> {
        let end = self.pos.checked_add(n).filter(|&end| end <= self.data.len()).ok_or_else(|| self.eof(what))?;
        let out = &self.data[self.pos..end];
        self.pos = end;
        Ok(out)
    }

    /// An unsigned varint (before zigzag decoding) of at most 10 bytes, truncated to 64 bits.
    ///
    /// The tenth byte carries bits 63..69 of the value, of which only bit 63 fits: the others are dropped, as the
    /// reference implementations do (Java's `BinaryDecoder.readLong`, fastavro, avro-c and the C++ extension all
    /// wrap). Writers do produce such values: pyiceberg zigzag-encodes a snapshot id above `INT64_MAX` into a 10-byte
    /// varint whose last byte sets bit 64, and every other reader turns it into the wrapped negative number. More than
    /// 10 bytes is still an error.
    #[inline]
    pub fn read_varint(&mut self) -> Result<u64> {
        // fast path: a single byte
        if let Some(&b) = self.data.get(self.pos)
            && b < 0x80
        {
            self.pos += 1;
            return Ok(u64::from(b));
        }
        let mut value: u64 = 0;
        let mut shift = 0u32;
        loop {
            let b = *self.data.get(self.pos).ok_or_else(|| self.eof("a varint"))?;
            self.pos += 1;
            // at shift 63 only the low bit of the byte remains in the u64: the shift discards the rest
            value |= u64::from(b & 0x7f) << shift;
            if b < 0x80 {
                return Ok(value);
            }
            shift += 7;
            if shift > 63 {
                return Err(invalid_input!("Avro varint longer than 10 bytes at offset {}", self.pos));
            }
        }
    }

    pub fn read_null(&mut self) -> Result<()> {
        Ok(())
    }

    pub fn read_boolean(&mut self) -> Result<bool> {
        match *self.data.get(self.pos).ok_or_else(|| self.eof("a boolean"))? {
            0 => {
                self.pos += 1;
                Ok(false)
            }
            1 => {
                self.pos += 1;
                Ok(true)
            }
            b => Err(invalid_input!("Avro boolean byte {b} at offset {}", self.pos)),
        }
    }

    #[inline]
    pub fn read_long(&mut self) -> Result<i64> {
        Ok(zigzag_decode(self.read_varint()?))
    }

    /// An `int`; the value must fit 32 bits.
    #[inline]
    pub fn read_int(&mut self) -> Result<i32> {
        let v = self.read_long()?;
        i32::try_from(v).map_err(|_| invalid_input!("Avro int {v} out of range at offset {}", self.pos))
    }

    pub fn read_float(&mut self) -> Result<f32> {
        let b = self.read_raw(4, "a float")?;
        Ok(f32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    }

    pub fn read_double(&mut self) -> Result<f64> {
        let b = self.read_raw(8, "a double")?;
        Ok(f64::from_le_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]]))
    }

    /// A non-negative length prefix that must fit the remaining bytes.
    #[inline]
    fn read_length(&mut self, what: &str) -> Result<usize> {
        let len = self.read_long()?;
        match usize::try_from(len) {
            Ok(len) if len <= self.remaining() => Ok(len),
            _ => Err(invalid_input!(
                "Avro {what} length {len} exceeds the {} remaining bytes at offset {}",
                self.remaining(),
                self.pos
            )),
        }
    }

    /// A `bytes` value.
    #[inline]
    pub fn read_bytes(&mut self) -> Result<&'a [u8]> {
        let len = self.read_length("bytes")?;
        self.read_raw(len, "bytes")
    }

    /// A `string` value, UTF-8 validated (the error text is [`INVALID_UNICODE`]).
    #[inline]
    pub fn read_string(&mut self) -> Result<&'a str> {
        let len = self.read_length("string")?;
        let raw = self.read_raw(len, "a string")?;
        std::str::from_utf8(raw).map_err(|_| Error::invalid_input(INVALID_UNICODE))
    }

    /// The raw bytes of a `string` value without UTF-8 validation (validate later with [`validate_utf8`]).
    #[inline]
    pub fn read_string_bytes(&mut self) -> Result<&'a [u8]> {
        let len = self.read_length("string")?;
        self.read_raw(len, "a string")
    }

    /// A `fixed` of `size` bytes.
    #[inline]
    pub fn read_fixed(&mut self, size: usize) -> Result<&'a [u8]> {
        self.read_raw(size, "a fixed")
    }

    /// An enum's symbol index, checked against `symbol_count`.
    pub fn read_enum(&mut self, symbol_count: usize) -> Result<usize> {
        let v = self.read_int()?;
        usize::try_from(v)
            .ok()
            .filter(|&i| i < symbol_count)
            .ok_or_else(|| invalid_input!("Enum value out of range: {v} (of {symbol_count} symbols)"))
    }

    /// A union's branch index, checked against `branch_count`.
    #[inline]
    pub fn read_union_index(&mut self, branch_count: usize) -> Result<usize> {
        let v = self.read_long()?;
        usize::try_from(v)
            .ok()
            .filter(|&i| i < branch_count)
            .ok_or_else(|| invalid_input!("Invalid union tag {v} (of {branch_count} branches)"))
    }

    /// The header of the next array or map block. A count of `0` ends the collection; a negative count in the
    /// encoding is returned as its magnitude with the byte size the writer recorded.
    ///
    /// The count is bounded by `min_item_size`, the fewest bytes one item of the block occupies
    /// ([`Schema::min_encoded_size`] of the item type, plus that of the key for a map): `count` items cannot need
    /// more than the remaining bytes, and a block of zero-size items may hold at most [`MAX_ZERO_SIZE_BLOCK_ITEMS`].
    /// A count beyond either is an invalid-input error, so a corrupt count never drives a loop the input cannot feed.
    /// Size allocations by `min(count, remaining())` all the same: items may be empty.
    #[inline]
    pub fn read_block_count(&mut self, min_item_size: usize) -> Result<BlockCount> {
        let count = self.read_long()?;
        let block = if count >= 0 {
            BlockCount { count: count as usize, byte_size: None }
        } else {
            let count = count.checked_neg().ok_or_else(|| Error::invalid_input("Avro block count overflow"))? as usize;
            let byte_size = self.read_length("block")?;
            BlockCount { count, byte_size: Some(byte_size) }
        };
        if min_item_size == 0 {
            if block.count > MAX_ZERO_SIZE_BLOCK_ITEMS {
                return Err(invalid_input!(
                    "Avro block count {} exceeds the limit of {MAX_ZERO_SIZE_BLOCK_ITEMS} zero-size items per block",
                    block.count
                ));
            }
        } else if block.count > self.remaining() / min_item_size {
            return Err(invalid_input!(
                "Avro block count {} exceeds the remaining {} bytes",
                block.count,
                self.remaining()
            ));
        }
        Ok(block)
    }

    /// Skips one value of the given schema without materializing it.
    pub fn skip(&mut self, schema: &Schema, id: SchemaId) -> Result<()> {
        self.skip_depth(schema, id, 0)
    }

    /// Skips one value of a leaf kind (everything but records, unions, arrays, maps and references); `None` for a
    /// compound kind. A leaf needs no validation to be skipped: any byte passes for a `boolean`, any varint for an
    /// `int` or `enum`, any bytes for a `string`.
    fn skip_leaf(&mut self, kind: &SchemaKind) -> Option<Result<()>> {
        Some(match kind {
            SchemaKind::Null => Ok(()),
            SchemaKind::Boolean => self.read_raw(1, "a boolean").map(|_| ()),
            SchemaKind::Int | SchemaKind::Long => self.read_varint().map(|_| ()),
            SchemaKind::Float => self.read_raw(4, "a float").map(|_| ()),
            SchemaKind::Double => self.read_raw(8, "a double").map(|_| ()),
            SchemaKind::Bytes | SchemaKind::String => self.read_bytes().map(|_| ()),
            SchemaKind::Fixed { size, .. } => self.read_raw(*size, "a fixed").map(|_| ()),
            SchemaKind::Enum { .. } => self.read_varint().map(|_| ()),
            _ => return None,
        })
    }

    fn skip_depth(&mut self, schema: &Schema, id: SchemaId, depth: usize) -> Result<()> {
        if depth > MAX_DEPTH {
            return Err(Error::invalid_input("Avro value nesting too deep"));
        }
        let kind = &schema[id].kind;
        if let Some(skipped) = self.skip_leaf(kind) {
            return skipped;
        }
        match kind {
            SchemaKind::Record { fields, .. } => {
                for field in fields {
                    self.skip_depth(schema, field.ty, depth + 1)?;
                }
                Ok(())
            }
            SchemaKind::Union { branches } => {
                let branch = self.read_union_index(branches.len())?;
                self.skip_depth(schema, branches[branch], depth + 1)
            }
            SchemaKind::Array { items, .. } => self.skip_blocks(schema, *items, false, depth),
            SchemaKind::Map { values, .. } => self.skip_blocks(schema, *values, true, depth),
            SchemaKind::Ref { target, .. } => self.skip_depth(schema, *target, depth + 1),
            // the leaf kinds: `skip_leaf` took them
            _ => Ok(()),
        }
    }

    fn skip_blocks(&mut self, schema: &Schema, item: SchemaId, keyed: bool, depth: usize) -> Result<()> {
        let min_item_size = schema.min_encoded_size(item).saturating_add(usize::from(keyed));
        loop {
            let block = self.read_block_count(min_item_size)?;
            if block.count == 0 {
                return Ok(());
            }
            if let Some(size) = block.byte_size {
                // the writer recorded the block's byte size: skip it wholesale
                self.read_raw(size, "an array block")?;
                continue;
            }
            for _ in 0..block.count {
                if keyed {
                    self.read_bytes()?;
                }
                self.skip_depth(schema, item, depth + 1)?;
            }
        }
    }
}

/// Validates bytes read with [`Decoder::read_string_bytes`] as UTF-8.
pub fn validate_utf8(bytes: &[u8]) -> Result<&str> {
    std::str::from_utf8(bytes).map_err(|_| Error::invalid_input(INVALID_UNICODE))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn varints() {
        let mut d = Decoder::new(&[
            0x00, 0x01, 0x02, 0x7f, 0x80, 0x01, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01,
        ]);
        assert_eq!(d.read_long().unwrap(), 0);
        assert_eq!(d.read_long().unwrap(), -1);
        assert_eq!(d.read_long().unwrap(), 1);
        assert_eq!(d.read_long().unwrap(), -64);
        assert_eq!(d.read_long().unwrap(), 64);
        assert_eq!(d.read_long().unwrap(), i64::MIN);
        assert!(d.is_empty());
        assert!(d.read_long().is_err());
        // 10 bytes with bits above the 64th set: truncated like the reference implementations do (pyiceberg's
        // encoding of 9876543210123456789, read as -653171173268680982 by Java and fastavro)
        let mut d = Decoder::new(&[0xab, 0xc4, 0xa3, 0x83, 0x96, 0xee, 0xc3, 0x90, 0x92, 0x02]);
        assert_eq!(d.read_long().unwrap(), -653171173268680982);
        assert!(d.is_empty());
        // the tenth byte's bits above its lowest are dropped: the same value as with 0x01
        let mut d = Decoder::new(&[0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f]);
        assert_eq!(d.read_long().unwrap(), i64::MIN);
        // more than 10 bytes is an error
        let mut d = Decoder::new(&[0xff; 11]);
        assert!(d.read_long().is_err());
        // an int wider than 32 bits
        let mut d = Decoder::new(&[0x80, 0x80, 0x80, 0x80, 0x20]);
        assert!(d.read_int().is_err());
    }

    #[test]
    fn lengths_are_bounded() {
        let mut d = Decoder::new(&[0x08, b'a', b'b']);
        assert!(d.read_bytes().is_err());
        let mut d = Decoder::new(&[0x04, 0xff, 0xfe]);
        assert_eq!(d.read_string().unwrap_err().message(), INVALID_UNICODE);
        let mut d = Decoder::new(&[0x03]);
        assert!(d.read_boolean().is_err());
    }
}

/// Kani proof harnesses (`cargo kani -p avro-core`, `make kani`): the decoder's varint and primitive readers over
/// every byte slice of a bounded length. Each harness fixes the slice length bound in its name or body; CBMC explores
/// all byte values and all lengths up to it.
#[cfg(kani)]
mod verification {
    use super::*;
    use crate::kani_support::{any_slice, well_formed_utf8};

    /// The value of the first varint in `data` computed independently with 128-bit arithmetic: `Some((value, bytes))`
    /// when a varint of at most 10 bytes ends in `data`, `None` when the data runs out or 10 bytes carry continuation
    /// bits. All bits are kept (7 per byte, 70 for 10 bytes); the decoder truncates to 64.
    fn reference_varint(data: &[u8]) -> Option<(u128, usize)> {
        let mut value: u128 = 0;
        let mut i = 0;
        while i < 10 {
            let b = *data.get(i)?;
            value |= u128::from(b & 0x7f) << (7 * i);
            i += 1;
            if b < 0x80 {
                return Some((value, i));
            }
        }
        None
    }

    /// `zigzag_decode(zigzag_encode(x)) == x` for every i64.
    #[kani::proof]
    fn zigzag_round_trip_i64() {
        let x: i64 = kani::any();
        assert_eq!(zigzag_decode(crate::encode::zigzag_encode(x)), x);
    }

    /// `zigzag_encode(zigzag_decode(v)) == v` for every u64 (every 64-bit varint payload is some long's encoding).
    #[kani::proof]
    fn zigzag_round_trip_u64() {
        let v: u64 = kani::any();
        assert_eq!(crate::encode::zigzag_encode(zigzag_decode(v)), v);
    }

    /// `read_varint` on any slice of at most 11 bytes: never panics, consumes at most 10 bytes, never moves past the
    /// end; it succeeds exactly when the reference parse does and then returns the reference value truncated to 64
    /// bits (the tenth byte contributes only its lowest bit: the Java / fastavro compatibility rule); 10 continuation
    /// bytes in a row are an error.
    #[kani::proof]
    #[kani::unwind(12)]
    fn read_varint_matches_reference() {
        let buf: [u8; 11] = kani::any();
        let data = any_slice(&buf);
        let mut dec = Decoder::new(data);
        let result = dec.read_varint();
        assert!(dec.position() <= data.len());
        match reference_varint(data) {
            Some((value, consumed)) => {
                assert_eq!(result.ok(), Some(value as u64));
                assert_eq!(dec.position(), consumed);
                assert!(consumed <= 10);
            }
            None => {
                assert!(result.is_err());
            }
        }
    }

    /// `read_long` is `zigzag_decode` of the varint; `read_int` accepts exactly the longs that fit an i32 and consumes
    /// the same bytes either way (an out-of-range int is an error, not a panic or a wrap).
    #[kani::proof]
    #[kani::unwind(12)]
    fn read_long_and_int_match_reference() {
        let buf: [u8; 11] = kani::any();
        let data = any_slice(&buf);
        let mut long_dec = Decoder::new(data);
        let long = long_dec.read_long();
        let mut int_dec = Decoder::new(data);
        let int = int_dec.read_int();
        assert!(long_dec.position() <= data.len());
        assert_eq!(int_dec.position(), long_dec.position());
        match reference_varint(data) {
            Some((value, _)) => {
                let expected = zigzag_decode(value as u64);
                assert_eq!(long.ok(), Some(expected));
                assert_eq!(int.ok(), i32::try_from(expected).ok());
            }
            None => {
                assert!(long.is_err());
                assert!(int.is_err());
            }
        }
    }

    /// `read_block_count(min_item_size)` on any slice of at most 12 bytes and any `min_item_size`: never panics, never
    /// moves past the end, and a returned count is one the remaining bytes can feed: `count * min_item_size <=
    /// remaining` for `min_item_size >= 1`, `count <= MAX_ZERO_SIZE_BLOCK_ITEMS` for 0. The negative form's byte size
    /// never exceeds the bytes left after it.
    #[kani::proof]
    #[kani::unwind(12)]
    fn read_block_count_is_bounded_by_remaining() {
        let buf: [u8; 12] = kani::any();
        let data = any_slice(&buf);
        let min_item_size: usize = kani::any();
        let mut dec = Decoder::new(data);
        let result = dec.read_block_count(min_item_size);
        assert!(dec.position() <= data.len());
        if let Ok(block) = result {
            let remaining = dec.remaining();
            if min_item_size == 0 {
                assert!(block.count <= MAX_ZERO_SIZE_BLOCK_ITEMS);
            } else {
                assert!(block.count.checked_mul(min_item_size).is_some_and(|bytes| bytes <= remaining));
            }
            if let Some(byte_size) = block.byte_size {
                assert!(byte_size <= remaining);
            }
        }
    }

    /// `read_boolean`: a 0 or 1 byte, anything else (including no byte) is an error; one byte consumed on success.
    #[kani::proof]
    fn read_boolean_never_panics() {
        let buf: [u8; 2] = kani::any();
        let data = any_slice(&buf);
        let mut dec = Decoder::new(data);
        let result = dec.read_boolean();
        assert!(dec.position() <= data.len());
        match data.first() {
            Some(&0) => {
                assert_eq!(result.ok(), Some(false));
            }
            Some(&1) => {
                assert_eq!(result.ok(), Some(true));
            }
            _ => {
                assert!(result.is_err() && dec.position() == 0);
            }
        }
    }

    /// `read_float` / `read_double`: exactly 4 / 8 little-endian bytes (bit pattern preserved, NaNs included), an
    /// error on fewer; `read_fixed(n)` for any `n` consumes exactly `n` bytes or fails without moving.
    #[kani::proof]
    #[kani::unwind(17)]
    fn read_float_double_fixed_never_panic() {
        let buf: [u8; 16] = kani::any();
        let data = any_slice(&buf);
        let mut dec = Decoder::new(data);
        let float = dec.read_float();
        assert_eq!(float.is_ok(), data.len() >= 4);
        if let Ok(f) = float {
            assert_eq!(f.to_bits(), u32::from_le_bytes([data[0], data[1], data[2], data[3]]));
            assert_eq!(dec.position(), 4);
        } else {
            assert_eq!(dec.position(), 0);
        }
        let mut dec = Decoder::new(data);
        let double = dec.read_double();
        assert_eq!(double.is_ok(), data.len() >= 8);
        if let Ok(d) = double {
            assert_eq!(d.to_bits(), u64::from_le_bytes(data[..8].try_into().unwrap()));
            assert_eq!(dec.position(), 8);
        } else {
            assert_eq!(dec.position(), 0);
        }
        let n: usize = kani::any();
        let mut dec = Decoder::new(data);
        let fixed = dec.read_fixed(n);
        assert_eq!(fixed.is_ok(), n <= data.len());
        assert_eq!(dec.position(), if n <= data.len() { n } else { 0 });
        if let Ok(fixed) = fixed {
            assert_eq!(fixed, &data[..n]);
        }
    }

    /// `read_bytes` on any slice of at most 16 bytes: never panics; succeeds exactly when the length prefix is a
    /// non-negative long that the remaining bytes cover, and then returns those bytes with the position at their end.
    #[kani::proof]
    #[kani::unwind(17)]
    fn read_bytes_never_panics() {
        let buf: [u8; 16] = kani::any();
        let data = any_slice(&buf);
        let mut dec = Decoder::new(data);
        let result = dec.read_bytes();
        assert!(dec.position() <= data.len());
        match reference_varint(data) {
            Some((value, consumed)) => {
                let len = zigzag_decode(value as u64);
                let fits = usize::try_from(len).is_ok_and(|len| len <= data.len() - consumed);
                assert_eq!(result.is_ok(), fits);
                if let Ok(bytes) = result {
                    assert_eq!(bytes, &data[consumed..consumed + len as usize]);
                    assert_eq!(dec.position(), consumed + len as usize);
                }
            }
            None => {
                assert!(result.is_err());
            }
        }
    }

    /// `read_string` on any slice of at most 8 bytes (a payload of up to 7: every 1- to 4-byte sequence, truncated
    /// or followed by another): never panics, succeeds exactly when the length-prefixed payload is there and is
    /// well-formed UTF-8 (an invalid payload is an error, not a panic), and then yields the payload. Bound: 8 bytes
    /// (`core::str::from_utf8` on more symbolic bytes exhausts CBMC).
    #[kani::proof]
    #[kani::unwind(9)]
    fn read_string_rejects_invalid_utf8_without_panicking() {
        let buf: [u8; 8] = kani::any();
        let data = any_slice(&buf);
        let mut dec = Decoder::new(data);
        let result = dec.read_string();
        assert!(dec.position() <= data.len());
        let mut raw_dec = Decoder::new(data);
        match raw_dec.read_bytes() {
            Ok(payload) => {
                assert_eq!(result.is_ok(), well_formed_utf8(payload));
                if let Ok(s) = result {
                    assert_eq!(s.as_bytes(), payload);
                    assert_eq!(dec.position(), raw_dec.position());
                }
            }
            Err(_) => {
                assert!(result.is_err());
            }
        }
    }

    /// `read_enum(symbol_count)` and `read_union_index(branch_count)` on any slice of at most 11 bytes and any count:
    /// never panic; succeed exactly when the decoded long is in `0..count` (an enum must also fit an int).
    #[kani::proof]
    #[kani::unwind(12)]
    fn read_enum_and_union_index_are_range_checked() {
        let buf: [u8; 11] = kani::any();
        let data = any_slice(&buf);
        let symbol_count: usize = kani::any();
        let branch_count: usize = kani::any();
        let mut enum_dec = Decoder::new(data);
        let symbol = enum_dec.read_enum(symbol_count);
        let mut union_dec = Decoder::new(data);
        let branch = union_dec.read_union_index(branch_count);
        assert!(enum_dec.position() <= data.len());
        assert!(union_dec.position() <= data.len());
        match reference_varint(data) {
            Some((value, consumed)) => {
                let long = zigzag_decode(value as u64);
                let in_symbols = i32::try_from(long).is_ok() && long >= 0 && (long as u128) < symbol_count as u128;
                let in_branches = long >= 0 && (long as u128) < branch_count as u128;
                assert_eq!(symbol.ok(), in_symbols.then_some(long as usize));
                assert_eq!(branch.ok(), in_branches.then_some(long as usize));
                assert_eq!(enum_dec.position(), consumed);
                assert_eq!(union_dec.position(), consumed);
            }
            None => {
                assert!(symbol.is_err());
                assert!(branch.is_err());
            }
        }
    }

    /// `Decoder::skip_leaf` of `kind` (what `skip` does for a leaf node) against `read` (the typed reader of that
    /// kind, `Ok(())` when it accepts the value) on every slice of at most 12 bytes: a value the reader accepts is
    /// skipped to the same position, `skip` fails only where the reader fails, and `skip` accepts exactly what `raw`
    /// (the unvalidated form of the read) accepts.
    fn check_skip(
        data: &[u8],
        kind: SchemaKind,
        read: impl FnOnce(&mut Decoder<'_>) -> Result<()>,
        raw: impl FnOnce(&mut Decoder<'_>) -> bool,
    ) {
        let mut skipper = Decoder::new(data);
        let skipped = skipper.skip_leaf(&kind).unwrap();
        let mut reader = Decoder::new(data);
        let read = read(&mut reader);
        assert!(skipper.position() <= data.len());
        if read.is_ok() {
            assert!(skipped.is_ok());
            assert_eq!(skipper.position(), reader.position());
        }
        if skipped.is_err() {
            assert!(read.is_err());
        }
        assert_eq!(skipped.is_ok(), raw(&mut Decoder::new(data)));
        std::mem::forget(kind);
    }

    /// `skip` of `null`, `boolean`, `float` and `double` against their readers on every slice of at most 12 bytes
    /// (`boolean` skips any byte: the reader's 0/1 check is the only difference).
    #[kani::proof]
    #[kani::unwind(3)]
    fn skip_matches_the_fixed_width_readers() {
        let buf: [u8; 12] = kani::any();
        let data = any_slice(&buf);
        let selector: u8 = kani::any();
        match selector {
            0 => check_skip(data, SchemaKind::Null, |d| d.read_null(), |_| true),
            1 => check_skip(data, SchemaKind::Boolean, |d| d.read_boolean().map(|_| ()), |d| d.read_raw(1, "").is_ok()),
            2 => check_skip(data, SchemaKind::Float, |d| d.read_float().map(|_| ()), |d| d.read_raw(4, "").is_ok()),
            _ => check_skip(data, SchemaKind::Double, |d| d.read_double().map(|_| ()), |d| d.read_raw(8, "").is_ok()),
        }
    }

    /// `skip` of `int`, `long` and `enum` (of every symbol count) against their readers on every slice of at most 12
    /// bytes: all three skip one varint, so `skip` is lenient exactly where the reader range-checks.
    #[kani::proof]
    #[kani::unwind(12)]
    fn skip_matches_the_varint_readers() {
        let buf: [u8; 12] = kani::any();
        let data = any_slice(&buf);
        let selector: u8 = kani::any();
        let count: usize = kani::any();
        let raw = |d: &mut Decoder<'_>| d.read_varint().is_ok();
        match selector {
            0 => check_skip(data, SchemaKind::Int, |d| d.read_int().map(|_| ()), raw),
            1 => check_skip(data, SchemaKind::Long, |d| d.read_long().map(|_| ()), raw),
            _ => check_skip(
                data,
                SchemaKind::Enum { name: crate::schema::Name::simple(""), symbols: Vec::new(), default: None },
                |d| d.read_enum(count).map(|_| ()),
                raw,
            ),
        }
    }

    /// `skip` of `bytes`, `string` and `fixed` (of every size <= 12) against their readers on every slice of at most
    /// 12 bytes (`string` through `read_string_bytes`: `skip` does not validate UTF-8, the harness above proves the
    /// validation).
    #[kani::proof]
    #[kani::unwind(13)]
    fn skip_matches_the_length_prefixed_readers() {
        let buf: [u8; 12] = kani::any();
        let data = any_slice(&buf);
        let selector: u8 = kani::any();
        let size: usize = kani::any();
        kani::assume(size <= 12);
        match selector {
            0 => check_skip(data, SchemaKind::Bytes, |d| d.read_bytes().map(|_| ()), |d| d.read_bytes().is_ok()),
            1 => {
                check_skip(data, SchemaKind::String, |d| d.read_string_bytes().map(|_| ()), |d| d.read_bytes().is_ok())
            }
            _ => check_skip(
                data,
                SchemaKind::Fixed { name: crate::schema::Name::simple(""), size },
                |d| d.read_fixed(size).map(|_| ()),
                |d| d.read_raw(size, "").is_ok(),
            ),
        }
    }
}
