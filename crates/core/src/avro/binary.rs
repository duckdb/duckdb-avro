use super::error::{AvroError, Result};

const MAX_VARINT_BYTES: usize = 10;

/// Reads Avro binary data from a byte slice.
#[derive(Debug, Clone)]
pub struct Decoder<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Decoder<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Decoder { data, pos: 0 }
    }

    pub fn position(&self) -> usize {
        self.pos
    }

    pub fn remaining(&self) -> usize {
        self.data.len() - self.pos
    }

    pub fn is_empty(&self) -> bool {
        self.remaining() == 0
    }

    /// Reads a zigzag-encoded variable-length long.
    pub fn read_long(&mut self) -> Result<i64> {
        let mut value: u64 = 0;
        let mut offset = 0;
        loop {
            if offset == MAX_VARINT_BYTES {
                return Err(AvroError::new("Varint too long"));
            }
            let byte = self.read_fixed(1)?[0];
            value |= u64::from(byte & 0x7F) << (7 * offset);
            offset += 1;
            if byte & 0x80 == 0 {
                break;
            }
        }
        Ok(((value >> 1) as i64) ^ -((value & 1) as i64))
    }

    pub fn read_int(&mut self) -> Result<i32> {
        let value = self.read_long()?;
        i32::try_from(value).map_err(|_| AvroError::new("Varint out of range for int type"))
    }

    /// Reads one byte; any non-zero value is true.
    pub fn read_boolean(&mut self) -> Result<bool> {
        Ok(self.read_fixed(1)?[0] != 0)
    }

    pub fn read_float(&mut self) -> Result<f32> {
        let bytes = self.read_fixed(4)?;
        Ok(f32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    pub fn read_double(&mut self) -> Result<f64> {
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(self.read_fixed(8)?);
        Ok(f64::from_le_bytes(bytes))
    }

    /// Reads a long length followed by that many bytes (also used for strings).
    pub fn read_bytes(&mut self) -> Result<&'a [u8]> {
        let len = self
            .read_long()
            .map_err(|e| e.prefixed("Cannot read bytes length: "))?;
        let len = usize::try_from(len).map_err(|_| AvroError::new("Invalid bytes value length"))?;
        self.read_fixed(len)
    }

    pub fn read_fixed(&mut self, size: usize) -> Result<&'a [u8]> {
        if self.remaining() < size {
            return Err(AvroError::new(format!(
                "Cannot read {size} bytes from memory buffer"
            )));
        }
        let bytes = &self.data[self.pos..self.pos + size];
        self.pos += size;
        Ok(bytes)
    }

    pub fn skip(&mut self, len: usize) -> Result<()> {
        if self.remaining() < len {
            return Err(AvroError::new(format!(
                "Cannot skip {len} bytes in memory buffer"
            )));
        }
        self.pos += len;
        Ok(())
    }
}

/// Appends a zigzag-encoded variable-length long.
pub fn write_long(out: &mut Vec<u8>, value: i64) {
    let mut n = ((value << 1) ^ (value >> 63)) as u64;
    while n & !0x7F != 0 {
        out.push((n as u8 & 0x7F) | 0x80);
        n >>= 7;
    }
    out.push(n as u8);
}

pub fn write_int(out: &mut Vec<u8>, value: i32) {
    write_long(out, i64::from(value));
}

pub fn write_boolean(out: &mut Vec<u8>, value: bool) {
    out.push(u8::from(value));
}

pub fn write_float(out: &mut Vec<u8>, value: f32) {
    out.extend_from_slice(&value.to_le_bytes());
}

pub fn write_double(out: &mut Vec<u8>, value: f64) {
    out.extend_from_slice(&value.to_le_bytes());
}

/// Appends a long length followed by the bytes (also used for strings).
pub fn write_bytes(out: &mut Vec<u8>, value: &[u8]) {
    write_long(out, value.len() as i64);
    out.extend_from_slice(value);
}

/// The number of bytes `write_long` emits for `value`.
pub fn long_size(value: i64) -> usize {
    let n = ((value << 1) ^ (value >> 63)) as u64;
    let bits = 64 - n.leading_zeros() as usize;
    bits.div_ceil(7).max(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encoded(value: i64) -> Vec<u8> {
        let mut out = Vec::new();
        write_long(&mut out, value);
        out
    }

    #[test]
    fn long_known_encodings() {
        assert_eq!(encoded(0), [0x00]);
        assert_eq!(encoded(-1), [0x01]);
        assert_eq!(encoded(1), [0x02]);
        assert_eq!(encoded(-64), [0x7F]);
        assert_eq!(encoded(64), [0x80, 0x01]);
        assert_eq!(encoded(i64::MAX).len(), 10);
        assert_eq!(
            encoded(i64::MIN),
            [0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01]
        );
    }

    #[test]
    fn long_round_trip_and_size() {
        let values = [
            0,
            1,
            -1,
            63,
            64,
            -64,
            -65,
            8191,
            8192,
            i64::from(i32::MAX),
            i64::from(i32::MIN),
            1 << 40,
            -(1 << 40),
            i64::MAX,
            i64::MIN,
        ];
        for value in values {
            let bytes = encoded(value);
            assert_eq!(long_size(value), bytes.len(), "{value}");
            let mut decoder = Decoder::new(&bytes);
            assert_eq!(decoder.read_long().unwrap(), value);
            assert!(decoder.is_empty());
        }
    }

    #[test]
    fn varint_too_long() {
        let bytes = [0x80u8; 11];
        let err = Decoder::new(&bytes).read_long().unwrap_err();
        assert_eq!(err.message, "Varint too long");
    }

    #[test]
    fn varint_truncated() {
        let err = Decoder::new(&[0x80, 0x80]).read_long().unwrap_err();
        assert_eq!(err.message, "Cannot read 1 bytes from memory buffer");
        let err = Decoder::new(&[]).read_long().unwrap_err();
        assert_eq!(err.message, "Cannot read 1 bytes from memory buffer");
    }

    #[test]
    fn int_range() {
        let mut out = Vec::new();
        write_int(&mut out, i32::MIN);
        write_long(&mut out, i64::from(i32::MAX) + 1);
        let mut decoder = Decoder::new(&out);
        assert_eq!(decoder.read_int().unwrap(), i32::MIN);
        assert_eq!(
            decoder.read_int().unwrap_err().message,
            "Varint out of range for int type"
        );
    }

    #[test]
    fn primitives_round_trip() {
        let mut out = Vec::new();
        write_boolean(&mut out, true);
        write_boolean(&mut out, false);
        write_float(&mut out, 1.5);
        write_double(&mut out, -2.25);
        write_bytes(&mut out, b"hello");
        write_bytes(&mut out, b"");
        out.extend_from_slice(b"fix");
        let mut decoder = Decoder::new(&out);
        assert!(decoder.read_boolean().unwrap());
        assert!(!decoder.read_boolean().unwrap());
        assert_eq!(decoder.read_float().unwrap(), 1.5);
        assert_eq!(decoder.read_double().unwrap(), -2.25);
        assert_eq!(decoder.read_bytes().unwrap(), b"hello");
        assert_eq!(decoder.read_bytes().unwrap(), b"");
        assert_eq!(decoder.position(), out.len() - 3);
        assert_eq!(decoder.remaining(), 3);
        assert_eq!(decoder.read_fixed(3).unwrap(), b"fix");
        assert!(decoder.is_empty());
    }

    #[test]
    fn boolean_non_zero_is_true() {
        assert!(Decoder::new(&[0x02]).read_boolean().unwrap());
    }

    #[test]
    fn little_endian_floats() {
        assert_eq!(Decoder::new(&[0, 0, 0x80, 0x3F]).read_float().unwrap(), 1.0);
        assert_eq!(
            Decoder::new(&[0, 0, 0, 0, 0, 0, 0xF0, 0x3F])
                .read_double()
                .unwrap(),
            1.0
        );
    }

    #[test]
    fn bytes_errors() {
        assert_eq!(
            Decoder::new(&[0x80]).read_bytes().unwrap_err().message,
            "Cannot read bytes length: Cannot read 1 bytes from memory buffer"
        );
        assert_eq!(
            Decoder::new(&[0x01]).read_bytes().unwrap_err().message,
            "Invalid bytes value length"
        );
        assert_eq!(
            Decoder::new(&[0x06, b'a'])
                .read_bytes()
                .unwrap_err()
                .message,
            "Cannot read 3 bytes from memory buffer"
        );
    }

    #[test]
    fn short_reads() {
        let mut decoder = Decoder::new(&[1, 2, 3]);
        assert_eq!(
            decoder.read_double().unwrap_err().message,
            "Cannot read 8 bytes from memory buffer"
        );
        assert_eq!(
            decoder.skip(4).unwrap_err().message,
            "Cannot skip 4 bytes in memory buffer"
        );
        decoder.skip(3).unwrap();
        assert!(decoder.is_empty());
        assert!(decoder.read_fixed(0).unwrap().is_empty());
    }
}
