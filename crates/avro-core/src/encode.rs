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

/// Kani proof harnesses (`cargo kani -p avro-core`, `make kani`): every primitive the [`Encoder`] writes is read back
/// unchanged by the [`crate::Decoder`], for all values of the fixed-width types and for all contents of the
/// variable-length ones up to the length bound each harness names. A `string` is the `bytes` round trip
/// ([`Encoder::write_string`] is `write_bytes_to` of the UTF-8) plus the decoder's UTF-8 check, proven in `decode`.
///
/// The encoders are created with a capacity no harness exceeds: a `Vec` growing at a symbolic length makes CBMC reason
/// about reallocation and copying at every possible size, which is not what these harnesses are about.
#[cfg(kani)]
mod verification {
    use super::*;
    use crate::decode::{BlockCount, Decoder, MAX_ZERO_SIZE_BLOCK_ITEMS};

    /// A slice of symbolic bytes and symbolic length `0..=N`.
    fn any_slice<const N: usize>(buf: &[u8; N]) -> &[u8] {
        let len: usize = kani::any();
        kani::assume(len <= N);
        &buf[..len]
    }

    /// A long encodes to at most 10 bytes and an int to at most 5, and both decode to the value written.
    #[kani::proof]
    #[kani::unwind(12)]
    fn long_and_int_round_trip_within_length_bounds() {
        let long: i64 = kani::any();
        let int: i32 = kani::any();
        assert!((1..=10).contains(&varint_bytes(zigzag_encode(long)).1));
        assert!((1..=5).contains(&varint_bytes(zigzag_encode(i64::from(int))).1));
        let mut enc = Encoder::with_capacity(64);
        enc.write_long(long);
        let long_len = enc.len();
        enc.write_int(int);
        assert!(long_len <= 10);
        assert!(enc.len() - long_len <= 5);
        let mut dec = Decoder::new(enc.as_bytes());
        assert_eq!(dec.read_long().unwrap(), long);
        assert_eq!(dec.read_int().unwrap(), int);
        assert!(dec.is_empty());
    }

    /// Boolean, float and double round trip bit-exactly (NaN payloads and the sign of zero included), in 1 + 4 + 8
    /// bytes.
    #[kani::proof]
    fn boolean_float_double_round_trip() {
        let b: bool = kani::any();
        let f = f32::from_bits(kani::any());
        let d = f64::from_bits(kani::any());
        let mut enc = Encoder::with_capacity(64);
        enc.write_boolean(b);
        enc.write_float(f);
        enc.write_double(d);
        assert_eq!(enc.len(), 13);
        let mut dec = Decoder::new(enc.as_bytes());
        assert_eq!(dec.read_boolean().unwrap(), b);
        assert_eq!(dec.read_float().unwrap().to_bits(), f.to_bits());
        assert_eq!(dec.read_double().unwrap().to_bits(), d.to_bits());
        assert!(dec.is_empty());
    }

    /// `bytes` of any content and length `0..=8` and a `fixed` of any content and size `0..=8` round trip.
    #[kani::proof]
    #[kani::unwind(10)]
    fn bytes_and_fixed_round_trip() {
        let bytes_buf: [u8; 8] = kani::any();
        let bytes = any_slice(&bytes_buf);
        let fixed_buf: [u8; 8] = kani::any();
        let fixed = any_slice(&fixed_buf);
        let mut enc = Encoder::with_capacity(64);
        enc.write_bytes(bytes);
        assert_eq!(enc.len(), 1 + bytes.len());
        enc.write_fixed(fixed);
        let mut dec = Decoder::new(enc.as_bytes());
        assert_eq!(dec.read_bytes().unwrap(), bytes);
        assert_eq!(dec.read_fixed(fixed.len()).unwrap(), fixed);
        assert!(dec.is_empty());
    }

    /// An enum symbol index and a union branch index round trip for every index below the count (the index itself
    /// bounded by `i64::MAX`, as a non-negative long).
    #[kani::proof]
    #[kani::unwind(12)]
    fn enum_and_union_index_round_trip() {
        let symbol_count: usize = kani::any();
        let symbol: usize = kani::any();
        kani::assume(symbol < symbol_count && symbol <= i32::MAX as usize);
        let branch_count: usize = kani::any();
        let branch: usize = kani::any();
        kani::assume(branch < branch_count && branch <= i64::MAX as usize);
        let mut enc = Encoder::with_capacity(64);
        enc.write_enum(symbol);
        enc.write_union_index(branch);
        let mut dec = Decoder::new(enc.as_bytes());
        assert_eq!(dec.read_enum(symbol_count).unwrap(), symbol);
        assert_eq!(dec.read_union_index(branch_count).unwrap(), branch);
        assert!(dec.is_empty());
    }

    /// The positive block count (any count `1..=MAX_ZERO_SIZE_BLOCK_ITEMS`, read with `min_item_size` 0) followed by
    /// the end marker round trips.
    #[kani::proof]
    #[kani::unwind(12)]
    fn positive_block_count_round_trips() {
        let count: usize = kani::any();
        kani::assume(count >= 1 && count <= MAX_ZERO_SIZE_BLOCK_ITEMS);
        let mut enc = Encoder::with_capacity(64);
        enc.write_block_count(count);
        enc.write_block_end();
        let mut dec = Decoder::new(enc.as_bytes());
        assert_eq!(dec.read_block_count(0).unwrap(), BlockCount { count, byte_size: None });
        assert_eq!(dec.read_block_count(0).unwrap(), BlockCount { count: 0, byte_size: None });
        assert!(dec.is_empty());
    }

    /// The negative block count with a byte size (any count `1..=8` and any size `count..=8`, read with
    /// `min_item_size` 1 over a payload of that size) followed by the end marker round trips. Needs about 17 GB of
    /// CBMC memory whatever the payload bound (the end marker's `push` after content of symbolic length), beyond a CI
    /// runner: behind the `kani-heavy` feature, which `make kani` enables and the CI job does not.
    #[cfg(feature = "kani-heavy")]
    #[kani::proof]
    #[kani::unwind(12)]
    fn sized_block_count_round_trips() {
        let count: usize = kani::any();
        let byte_size: usize = kani::any();
        kani::assume(count >= 1 && count <= byte_size && byte_size <= 8);
        let mut enc = Encoder::with_capacity(64);
        enc.write_block_count_with_size(count, byte_size);
        enc.write_raw(&[0u8; 8][..byte_size]);
        enc.write_block_end();
        let mut dec = Decoder::new(enc.as_bytes());
        assert_eq!(dec.read_block_count(1).unwrap(), BlockCount { count, byte_size: Some(byte_size) });
        dec.read_raw(byte_size, "the block").unwrap();
        assert_eq!(dec.read_block_count(1).unwrap(), BlockCount { count: 0, byte_size: None });
        assert!(dec.is_empty());
    }

    /// A `long` is the spec's zigzag varint for every i64: the zigzag value is `2v` for `v >= 0` and `2|v| - 1`
    /// below it (the sign lands in bit 0), byte `i` of the encoding carries its bits `7i..7i+7` with the high bit set
    /// exactly on the bytes before the last, and the last byte is the first after which no bits remain (no over-long
    /// encoding); 1 to 10 bytes. Independent of the decoder, which the round-trip harnesses go through.
    #[kani::proof]
    #[kani::unwind(12)]
    fn long_bytes_are_the_spec_varint() {
        let value: i64 = kani::any();
        let z = zigzag_encode(value);
        let expected_z = if value >= 0 { 2 * (value as u128) } else { 2 * ((-(value as i128)) as u128) - 1 };
        assert_eq!(u128::from(z), expected_z);
        let mut enc = Encoder::with_capacity(16);
        enc.write_long(value);
        let bytes = enc.as_bytes();
        let n = bytes.len();
        assert!((1..=10).contains(&n));
        let mut i = 0;
        while i < n {
            assert_eq!(bytes[i] & 0x7f, ((z >> (7 * i)) & 0x7f) as u8);
            assert_eq!(bytes[i] & 0x80 != 0, i + 1 < n);
            i += 1;
        }
        assert!(n == 10 || z >> (7 * n) == 0);
        assert!(n == 1 || z >> (7 * (n - 1)) != 0);
    }

    /// The fixed-width values are the spec's bytes: `boolean` the one byte 0 or 1, `float` and `double` their IEEE
    /// bits little-endian (byte `k` is bits `8k..8k+8`), the block end marker the byte 0, `null` nothing.
    #[kani::proof]
    fn fixed_width_values_are_the_spec_bytes() {
        let b: bool = kani::any();
        let f: u32 = kani::any();
        let d: u64 = kani::any();
        let mut enc = Encoder::with_capacity(16);
        enc.write_null();
        enc.write_boolean(b);
        enc.write_float(f32::from_bits(f));
        enc.write_double(f64::from_bits(d));
        enc.write_block_end();
        let bytes = enc.as_bytes();
        assert_eq!(bytes.len(), 14);
        assert_eq!(bytes[0], if b { 1 } else { 0 });
        let mut k = 0;
        while k < 4 {
            assert_eq!(bytes[1 + k], (f >> (8 * k)) as u8);
            k += 1;
        }
        let mut k = 0;
        while k < 8 {
            assert_eq!(bytes[5 + k], (d >> (8 * k)) as u8);
            k += 1;
        }
        assert_eq!(bytes[13], 0);
    }
}
