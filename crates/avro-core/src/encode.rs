//! The Avro binary encoder: appends values to an in-memory buffer the container writer turns into a block.

/// Encodes `value` as the zigzag varint payload the Avro spec defines.
#[inline]
pub fn zigzag_encode(value: i64) -> u64 {
    ((value << 1) ^ (value >> 63)) as u64
}

/// The unsigned varint of `value`: its bytes in a 10-byte array and how many of them there are (`1..=10`).
#[inline]
fn varint_bytes(mut value: u64) -> ([u8; 10], usize) {
    let mut bytes = [0u8; 10];
    let mut n = 0;
    while value >= 0x80 {
        bytes[n] = (value as u8) | 0x80;
        value >>= 7;
        n += 1;
    }
    bytes[n] = value as u8;
    (bytes, n + 1)
}

/// Appends an unsigned varint to `out` (one reservation for all its bytes).
#[inline]
pub fn write_varint(out: &mut Vec<u8>, value: u64) {
    let (bytes, n) = varint_bytes(value);
    out.extend_from_slice(&bytes[..n]);
}

/// Appends a zigzag varint `long` to `out`.
#[inline]
pub fn write_long_to(out: &mut Vec<u8>, value: i64) {
    write_varint(out, zigzag_encode(value));
}

/// Appends a length-prefixed byte string to `out`.
pub fn write_bytes_to(out: &mut Vec<u8>, bytes: &[u8]) {
    write_long_to(out, bytes.len() as i64);
    out.extend_from_slice(bytes);
}

/// An encoder over an owned buffer; the block-oriented container writer takes the buffer when a block is complete.
#[derive(Debug, Default, Clone)]
pub struct Encoder {
    buf: Vec<u8>,
}

impl Encoder {
    pub fn new() -> Self {
        Encoder { buf: Vec::new() }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Encoder { buf: Vec::with_capacity(capacity) }
    }

    /// Bytes encoded so far.
    pub fn len(&self) -> usize {
        self.buf.len()
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.buf
    }

    /// Takes the buffer, leaving the encoder empty (capacity retained).
    pub fn take(&mut self) -> Vec<u8> {
        std::mem::take(&mut self.buf)
    }

    pub fn into_inner(self) -> Vec<u8> {
        self.buf
    }

    pub fn clear(&mut self) {
        self.buf.clear();
    }

    pub fn write_null(&mut self) {}

    #[inline]
    pub fn write_boolean(&mut self, value: bool) {
        self.buf.push(u8::from(value));
    }

    #[inline]
    pub fn write_int(&mut self, value: i32) {
        write_long_to(&mut self.buf, i64::from(value));
    }

    #[inline]
    pub fn write_long(&mut self, value: i64) {
        write_long_to(&mut self.buf, value);
    }

    #[inline]
    pub fn write_float(&mut self, value: f32) {
        self.buf.extend_from_slice(&value.to_le_bytes());
    }

    #[inline]
    pub fn write_double(&mut self, value: f64) {
        self.buf.extend_from_slice(&value.to_le_bytes());
    }

    #[inline]
    pub fn write_bytes(&mut self, value: &[u8]) {
        write_bytes_to(&mut self.buf, value);
    }

    #[inline]
    pub fn write_string(&mut self, value: &str) {
        write_bytes_to(&mut self.buf, value.as_bytes());
    }

    /// A `fixed`: the caller guarantees `value.len()` equals the schema's size.
    #[inline]
    pub fn write_fixed(&mut self, value: &[u8]) {
        self.buf.extend_from_slice(value);
    }

    #[inline]
    pub fn write_enum(&mut self, index: usize) {
        write_long_to(&mut self.buf, index as i64);
    }

    #[inline]
    pub fn write_union_index(&mut self, index: usize) {
        write_long_to(&mut self.buf, index as i64);
    }

    /// Starts an array or map block of `count` items (`count > 0`; call [`Encoder::write_block_end`] after the
    /// last block).
    #[inline]
    pub fn write_block_count(&mut self, count: usize) {
        write_long_to(&mut self.buf, count as i64);
    }

    /// Starts a block of `count` items whose encoded items occupy `byte_size` bytes (the negative-count form that
    /// lets readers skip the block without decoding it).
    pub fn write_block_count_with_size(&mut self, count: usize, byte_size: usize) {
        write_long_to(&mut self.buf, -(count as i64));
        write_long_to(&mut self.buf, byte_size as i64);
    }

    /// Ends an array or map (the zero count).
    #[inline]
    pub fn write_block_end(&mut self) {
        self.buf.push(0);
    }

    /// Appends already encoded bytes.
    pub fn write_raw(&mut self, bytes: &[u8]) {
        self.buf.extend_from_slice(bytes);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::decode::Decoder;

    #[test]
    fn round_trip() {
        let mut e = Encoder::new();
        for v in [0, -1, 1, i64::MIN, i64::MAX, 300, -300] {
            e.write_long(v);
        }
        e.write_string("héllo");
        e.write_boolean(true);
        e.write_double(1.5);
        e.write_float(-2.25);
        let mut d = Decoder::new(e.as_bytes());
        for v in [0, -1, 1, i64::MIN, i64::MAX, 300, -300] {
            assert_eq!(d.read_long().unwrap(), v);
        }
        assert_eq!(d.read_string().unwrap(), "héllo");
        assert!(d.read_boolean().unwrap());
        assert_eq!(d.read_double().unwrap(), 1.5);
        assert_eq!(d.read_float().unwrap(), -2.25);
        assert!(d.is_empty());
    }
}
