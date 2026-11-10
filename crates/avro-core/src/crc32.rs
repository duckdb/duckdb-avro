//! Table-based CRC-32 (IEEE 802.3, reflected polynomial 0xEDB88320), as used by the snappy codec trailer.

const fn make_table() -> [u32; 256] {
    let mut table = [0u32; 256];
    let mut i = 0;
    while i < 256 {
        let mut crc = i as u32;
        let mut bit = 0;
        while bit < 8 {
            crc = if crc & 1 != 0 { (crc >> 1) ^ 0xEDB8_8320 } else { crc >> 1 };
            bit += 1;
        }
        table[i] = crc;
        i += 1;
    }
    table
}

static TABLE: [u32; 256] = make_table();

/// The CRC-32 of `data` (same value as zlib's `crc32(0, data, len)`).
pub fn crc32(data: &[u8]) -> u32 {
    let mut crc = 0xFFFF_FFFFu32;
    for &b in data {
        crc = TABLE[((crc ^ u32::from(b)) & 0xFF) as usize] ^ (crc >> 8);
    }
    !crc
}

#[cfg(test)]
mod tests {
    #[test]
    fn known_vectors() {
        assert_eq!(super::crc32(b""), 0);
        assert_eq!(super::crc32(b"123456789"), 0xCBF4_3926);
        assert_eq!(super::crc32(b"The quick brown fox jumps over the lazy dog"), 0x414F_A339);
    }
}

/// Kani proof harnesses (`cargo kani -p avro-core`, `make kani`): the table and the table-driven CRC agree with the
/// bitwise definition of the reflected CRC-32 (polynomial 0xEDB88320, initial value and final xor 0xFFFFFFFF).
#[cfg(kani)]
mod verification {
    /// One byte folded into `crc` bit by bit, the definition the table shortcuts.
    fn bitwise_update(mut crc: u32, byte: u8) -> u32 {
        crc ^= u32::from(byte);
        let mut bit = 0;
        while bit < 8 {
            crc = if crc & 1 != 0 { (crc >> 1) ^ 0xEDB8_8320 } else { crc >> 1 };
            bit += 1;
        }
        crc
    }

    /// Every one of the 256 table entries is the bitwise CRC of its index.
    #[kani::proof]
    #[kani::unwind(9)]
    fn table_entries_match_bitwise_definition() {
        let i: u8 = kani::any();
        assert_eq!(super::TABLE[usize::from(i)], bitwise_update(0, i));
    }

    /// `crc32` of any one or two bytes equals the bitwise computation over the same bytes (the single byte exercises
    /// all 256 table lookups through the initial value; the second byte checks the shift-and-fold of a running CRC).
    #[kani::proof]
    #[kani::unwind(9)]
    fn crc32_of_short_inputs_matches_bitwise_definition() {
        let b0: u8 = kani::any();
        let b1: u8 = kani::any();
        assert_eq!(super::crc32(&[b0]), !bitwise_update(0xFFFF_FFFF, b0));
        assert_eq!(super::crc32(&[b0, b1]), !bitwise_update(bitwise_update(0xFFFF_FFFF, b0), b1));
        assert_eq!(super::crc32(&[]), 0);
    }
}
