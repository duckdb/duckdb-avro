//! The Avro binary decoder: a pull API over a slice of decoded block bytes.
//!
//! The schema-driven consumer calls the typed `read_*` methods in schema order; nothing is materialized. Every read
//! is bounds-checked against the slice, so a truncated or corrupt block yields an error instead of a panic.

use crate::error::{Error, Result};
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
        Error::invalid_input(format!("Avro block truncated while reading {what} at offset {}", self.pos))
    }

    /// Takes `n` raw bytes.
    #[inline]
    pub fn read_raw(&mut self, n: usize, what: &str) -> Result<&'a [u8]> {
        let end = self.pos.checked_add(n).filter(|&end| end <= self.data.len()).ok_or_else(|| self.eof(what))?;
        let out = &self.data[self.pos..end];
        self.pos = end;
        Ok(out)
    }

    /// An unsigned varint (before zigzag decoding), at most 10 bytes, 64-bit overflow detected.
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
            if shift == 63 && b > 1 {
                return Err(Error::invalid_input(format!("Avro varint overflows 64 bits at offset {}", self.pos)));
            }
            value |= u64::from(b & 0x7f) << shift;
            if b < 0x80 {
                return Ok(value);
            }
            shift += 7;
            if shift > 63 {
                return Err(Error::invalid_input(format!("Avro varint longer than 10 bytes at offset {}", self.pos)));
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
            b => Err(Error::invalid_input(format!("Avro boolean byte {b} at offset {}", self.pos))),
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
        i32::try_from(v).map_err(|_| Error::invalid_input(format!("Avro int {v} out of range at offset {}", self.pos)))
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
            _ => Err(Error::invalid_input(format!(
                "Avro {what} length {len} exceeds the {} remaining bytes at offset {}",
                self.remaining(),
                self.pos
            ))),
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
            .ok_or_else(|| Error::invalid_input(format!("Enum value out of range: {v} (of {symbol_count} symbols)")))
    }

    /// A union's branch index, checked against `branch_count`.
    #[inline]
    pub fn read_union_index(&mut self, branch_count: usize) -> Result<usize> {
        let v = self.read_long()?;
        usize::try_from(v)
            .ok()
            .filter(|&i| i < branch_count)
            .ok_or_else(|| Error::invalid_input(format!("Invalid union tag {v} (of {branch_count} branches)")))
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
                return Err(Error::invalid_input(format!(
                    "Avro block count {} exceeds the limit of {MAX_ZERO_SIZE_BLOCK_ITEMS} zero-size items per block",
                    block.count
                )));
            }
        } else if block.count > self.remaining() / min_item_size {
            return Err(Error::invalid_input(format!(
                "Avro block count {} exceeds the remaining {} bytes",
                block.count,
                self.remaining()
            )));
        }
        Ok(block)
    }

    /// Skips one value of the given schema without materializing it.
    pub fn skip(&mut self, schema: &Schema, id: SchemaId) -> Result<()> {
        self.skip_depth(schema, id, 0)
    }

    fn skip_depth(&mut self, schema: &Schema, id: SchemaId, depth: usize) -> Result<()> {
        if depth > MAX_DEPTH {
            return Err(Error::invalid_input("Avro value nesting too deep"));
        }
        match &schema[id].kind {
            SchemaKind::Null => Ok(()),
            SchemaKind::Boolean => self.read_raw(1, "a boolean").map(|_| ()),
            SchemaKind::Int | SchemaKind::Long => self.read_varint().map(|_| ()),
            SchemaKind::Float => self.read_raw(4, "a float").map(|_| ()),
            SchemaKind::Double => self.read_raw(8, "a double").map(|_| ()),
            SchemaKind::Bytes | SchemaKind::String => self.read_bytes().map(|_| ()),
            SchemaKind::Fixed { size, .. } => self.read_raw(*size, "a fixed").map(|_| ()),
            SchemaKind::Enum { .. } => self.read_varint().map(|_| ()),
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
        // overflow: 10 bytes with the last above 1
        let mut d = Decoder::new(&[0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x02]);
        assert!(d.read_long().is_err());
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
