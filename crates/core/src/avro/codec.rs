use std::io::{Read, Write};

use super::error::{AvroError, Result};

const LZMA_DICT_SIZE: u32 = 8 << 20;
const LZMA_PRESET: u32 = 6;
const BZIP2_LEVEL: u32 = 9;
const DEFLATE_LEVEL: u32 = 9;
const INITIAL_OUTPUT_SIZE: usize = 16 * 1024;

/// A block compression codec of an object container file (`avro.codec`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Codec {
    Null,
    Deflate,
    Snappy,
    Lzma,
    Zstandard,
    Bzip2,
    Xz,
}

impl Codec {
    /// The codec of an `avro.codec` name, case insensitively; `"Unknown codec <name>"` for others.
    pub fn from_name(name: &str) -> Result<Codec> {
        match name.to_ascii_lowercase().as_str() {
            "null" => Ok(Codec::Null),
            "deflate" => Ok(Codec::Deflate),
            "snappy" => Ok(Codec::Snappy),
            "lzma" => Ok(Codec::Lzma),
            "zstandard" => Ok(Codec::Zstandard),
            "bzip2" => Ok(Codec::Bzip2),
            "xz" => Ok(Codec::Xz),
            _ => Err(AvroError::new(format!("Unknown codec {name}"))),
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Codec::Null => "null",
            Codec::Deflate => "deflate",
            Codec::Snappy => "snappy",
            Codec::Lzma => "lzma",
            Codec::Zstandard => "zstandard",
            Codec::Bzip2 => "bzip2",
            Codec::Xz => "xz",
        }
    }

    /// Compresses one block, replacing the contents of `out`.
    pub fn encode(self, data: &[u8], out: &mut Vec<u8>) -> Result<()> {
        out.clear();
        match self {
            Codec::Null => out.extend_from_slice(data),
            Codec::Deflate => encode_deflate(data, out)?,
            Codec::Snappy => encode_snappy(data, out)?,
            Codec::Lzma => encode_lzma(data, out)?,
            Codec::Zstandard => ruzstd::encoding::compress(
                data,
                &mut *out,
                ruzstd::encoding::CompressionLevel::Fastest,
            ),
            Codec::Bzip2 => encode_bzip2(data, out)?,
            Codec::Xz => encode_xz(data, out)?,
        }
        Ok(())
    }

    /// Decompresses one block into `out` (cleared first) and returns it as a slice; `Null` may return `data` itself.
    pub fn decode<'a>(self, data: &'a [u8], out: &'a mut Vec<u8>) -> Result<&'a [u8]> {
        out.clear();
        match self {
            Codec::Null => return Ok(data),
            Codec::Deflate => decode_deflate(data, out)?,
            Codec::Snappy => decode_snappy(data, out)?,
            Codec::Lzma => decode_lzma(data, out)?,
            Codec::Zstandard => decode_zstandard(data, out)?,
            Codec::Bzip2 => decode_bzip2(data, out)?,
            Codec::Xz => decode_xz(data, out)?,
        }
        Ok(out)
    }
}

fn encode_deflate(data: &[u8], out: &mut Vec<u8>) -> Result<()> {
    let mut encoder =
        flate2::write::DeflateEncoder::new(out, flate2::Compression::new(DEFLATE_LEVEL));
    encoder
        .write_all(data)
        .and_then(|()| encoder.try_finish())
        .map_err(|e| AvroError::new(format!("Error compressing block with deflate ({e})")))
}

fn decode_deflate(data: &[u8], out: &mut Vec<u8>) -> Result<()> {
    let data_error =
        || AvroError::new("Error decompressing block with deflate, possible data error");
    let mut inflater = flate2::Decompress::new(false);
    out.reserve(INITIAL_OUTPUT_SIZE.max(data.len().saturating_mul(4)));
    loop {
        if out.len() == out.capacity() {
            out.reserve(out.capacity());
        }
        let consumed = inflater.total_in() as usize;
        let produced = out.len();
        let status = inflater
            .decompress_vec(&data[consumed..], out, flate2::FlushDecompress::None)
            .map_err(|_| data_error())?;
        if status == flate2::Status::StreamEnd {
            return Ok(());
        }
        if inflater.total_in() as usize == consumed && out.len() == produced {
            return Err(data_error());
        }
    }
}

fn encode_snappy(data: &[u8], out: &mut Vec<u8>) -> Result<()> {
    out.resize(snap::raw::max_compress_len(data.len()), 0);
    let written = snap::raw::Encoder::new()
        .compress(data, out)
        .map_err(|_| AvroError::new("Error compressing block with Snappy"))?;
    out.truncate(written);
    out.extend_from_slice(&crc32fast::hash(data).to_be_bytes());
    Ok(())
}

fn decode_snappy(data: &[u8], out: &mut Vec<u8>) -> Result<()> {
    let length_error = || AvroError::new("Uncompressed length error in snappy");
    let Some(split) = data.len().checked_sub(4) else {
        return Err(length_error());
    };
    let (compressed, crc) = data.split_at(split);
    let len = snap::raw::decompress_len(compressed).map_err(|_| length_error())?;
    // A snappy copy element of at least two bytes expands to at most 64, so this bounds what valid data decodes to
    if len > compressed.len().saturating_mul(32).saturating_add(64) {
        return Err(length_error());
    }
    out.resize(len, 0);
    let written = snap::raw::Decoder::new()
        .decompress(compressed, out)
        .map_err(|_| AvroError::new("Error uncompressing block with Snappy"))?;
    out.truncate(written);
    if crc32fast::hash(out).to_be_bytes() != crc {
        return Err(AvroError::new(
            "CRC32 check failure uncompressing block with Snappy",
        ));
    }
    Ok(())
}

fn encode_lzma(data: &[u8], out: &mut Vec<u8>) -> Result<()> {
    let encoder_error = |_| AvroError::new("Error in lzma encoder");
    let mut options = lzma_rust2::Lzma2Options::with_preset(LZMA_PRESET);
    options.lzma_options.dict_size = LZMA_DICT_SIZE;
    let mut encoder = lzma_rust2::Lzma2Writer::new(out, options);
    encoder.write_all(data).map_err(encoder_error)?;
    encoder.finish().map_err(encoder_error)?;
    Ok(())
}

fn decode_lzma(data: &[u8], out: &mut Vec<u8>) -> Result<()> {
    lzma_rust2::Lzma2Reader::new(data, LZMA_DICT_SIZE, None)
        .read_to_end(out)
        .map_err(|_| AvroError::new("Error in lzma decoder"))?;
    Ok(())
}

fn encode_xz(data: &[u8], out: &mut Vec<u8>) -> Result<()> {
    let encoder_error = |_| AvroError::new("Error in xz encoder");
    let mut options = lzma_rust2::XzOptions::with_preset(LZMA_PRESET);
    options.lzma_options.dict_size = LZMA_DICT_SIZE;
    let mut encoder = lzma_rust2::XzWriter::new(out, options).map_err(encoder_error)?;
    encoder.write_all(data).map_err(encoder_error)?;
    encoder.finish().map_err(encoder_error)?;
    Ok(())
}

fn decode_xz(data: &[u8], out: &mut Vec<u8>) -> Result<()> {
    lzma_rust2::XzReader::new(data, true)
        .read_to_end(out)
        .map_err(|e| AvroError::new(format!("Error uncompressing block with xz: {e}")))?;
    Ok(())
}

fn decode_zstandard(mut data: &[u8], out: &mut Vec<u8>) -> Result<()> {
    use ruzstd::decoding::errors::{FrameDecoderError, ReadFrameHeaderError};
    use ruzstd::decoding::{BlockDecodingStrategy, FrameDecoder};

    let zstd_error = |e: &dyn std::fmt::Display| {
        AvroError::new(format!("Error uncompressing block with zstandard: {e}"))
    };
    let mut decoder = FrameDecoder::new();
    while !data.is_empty() {
        match decoder.reset(&mut data) {
            Ok(()) => {}
            Err(FrameDecoderError::ReadFrameHeaderError(ReadFrameHeaderError::SkipFrame {
                length,
                ..
            })) => {
                data = data
                    .get(length as usize..)
                    .ok_or_else(|| AvroError::new("Truncated zstandard block"))?;
                continue;
            }
            Err(e) => return Err(zstd_error(&e)),
        }
        loop {
            let finished = decoder
                .decode_blocks(&mut data, BlockDecodingStrategy::UptoBytes(1 << 20))
                .map_err(|e| zstd_error(&e))?;
            decoder
                .collect_to_writer(&mut *out)
                .map_err(|e| zstd_error(&e))?;
            if finished {
                break;
            }
        }
        decoder
            .collect_to_writer(&mut *out)
            .map_err(|e| zstd_error(&e))?;
        if let Some(expected) = decoder.get_checksum_from_data()
            && decoder.get_calculated_checksum() != Some(expected)
        {
            return Err(zstd_error(&"Restored data doesn't match checksum"));
        }
    }
    Ok(())
}

fn encode_bzip2(data: &[u8], out: &mut Vec<u8>) -> Result<()> {
    let mut encoder = bzip2::write::BzEncoder::new(out, bzip2::Compression::new(BZIP2_LEVEL));
    encoder
        .write_all(data)
        .and_then(|()| encoder.try_finish())
        .map_err(|e| AvroError::new(format!("Error compressing block with bzip2: {e}")))
}

fn decode_bzip2(data: &[u8], out: &mut Vec<u8>) -> Result<()> {
    bzip2::read::BzDecoder::new(data)
        .read_to_end(out)
        .map_err(|e| AvroError::new(format!("Error uncompressing block with bzip2: {e}")))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const ALL: [Codec; 7] = [
        Codec::Null,
        Codec::Deflate,
        Codec::Snappy,
        Codec::Lzma,
        Codec::Zstandard,
        Codec::Bzip2,
        Codec::Xz,
    ];

    #[test]
    fn snappy_rejects_impossible_lengths() {
        // A length header of 4 GiB in front of a few bytes of data
        let mut data = vec![0x80, 0x80, 0x80, 0x80, 0x0f, 0, 0];
        data.extend_from_slice(&[0; 4]);
        let mut out = Vec::new();
        let err = Codec::Snappy.decode(&data, &mut out).unwrap_err();
        assert_eq!(err.message, "Uncompressed length error in snappy");
        assert!(out.capacity() < 1 << 20);
    }

    fn sample(len: usize) -> Vec<u8> {
        let mut state: u32 = 12345;
        (0..len)
            .map(|i| {
                state = state.wrapping_mul(1_103_515_245).wrapping_add(12345);
                if i % 3 == 0 {
                    (state >> 24) as u8
                } else {
                    (i / 7) as u8
                }
            })
            .collect()
    }

    fn round_trip(codec: Codec, data: &[u8]) {
        let mut compressed = Vec::new();
        codec.encode(data, &mut compressed).unwrap();
        let mut scratch = vec![1, 2, 3];
        let decoded = codec.decode(&compressed, &mut scratch).unwrap();
        assert!(
            decoded == data,
            "{codec:?} round trip of {} bytes",
            data.len()
        );
    }

    #[test]
    fn names() {
        for codec in ALL {
            assert_eq!(Codec::from_name(codec.name()).unwrap(), codec);
        }
        assert_eq!(Codec::from_name("Deflate").unwrap(), Codec::Deflate);
        assert_eq!(Codec::from_name("XZ").unwrap(), Codec::Xz);
        assert_eq!(
            Codec::from_name(" deflate").unwrap_err().message,
            "Unknown codec  deflate"
        );
        assert_eq!(
            Codec::from_name("brotli").unwrap_err().message,
            "Unknown codec brotli"
        );
    }

    #[test]
    fn round_trip_empty() {
        for codec in ALL {
            round_trip(codec, b"");
        }
    }

    #[test]
    fn round_trip_small() {
        for codec in ALL {
            round_trip(codec, b"a");
            round_trip(codec, b"hello hello hello hello hello");
        }
    }

    #[test]
    fn round_trip_multi_megabyte() {
        let data = sample(5 << 20);
        for codec in ALL {
            round_trip(codec, &data);
        }
    }

    #[test]
    fn round_trip_highly_compressible() {
        let data = vec![7u8; 3 << 20];
        for codec in ALL {
            round_trip(codec, &data);
        }
    }

    #[test]
    fn encode_replaces_output() {
        let mut out = vec![9; 100];
        Codec::Null.encode(b"abc", &mut out).unwrap();
        assert_eq!(out, b"abc");
        Codec::Snappy.encode(b"abc", &mut out).unwrap();
        assert_eq!(&out[..5], [3, 8, b'a', b'b', b'c']);
    }

    #[test]
    fn null_decode_borrows_input() {
        let data = b"abc";
        let mut scratch = Vec::new();
        let decoded = Codec::Null.decode(data, &mut scratch).unwrap();
        assert_eq!(decoded.as_ptr(), data.as_ptr());
    }

    #[test]
    fn snappy_crc_layout() {
        let mut out = Vec::new();
        Codec::Snappy.encode(b"123456789", &mut out).unwrap();
        let mut expected = vec![9, 8 << 2];
        expected.extend_from_slice(b"123456789");
        expected.extend_from_slice(&[0xCB, 0xF4, 0x39, 0x26]);
        assert_eq!(out, expected);
    }

    #[test]
    fn snappy_errors() {
        let mut scratch = Vec::new();
        for short in [&b""[..], b"\x00", b"\x00\x00\x00"] {
            assert_eq!(
                Codec::Snappy
                    .decode(short, &mut scratch)
                    .unwrap_err()
                    .message,
                "Uncompressed length error in snappy"
            );
        }
        let mut block = Vec::new();
        Codec::Snappy.encode(b"123456789", &mut block).unwrap();
        let last = block.len() - 1;
        block[last] ^= 1;
        assert_eq!(
            Codec::Snappy
                .decode(&block, &mut scratch)
                .unwrap_err()
                .message,
            "CRC32 check failure uncompressing block with Snappy"
        );
        assert_eq!(
            Codec::Snappy
                .decode(
                    &[0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0, 0, 0, 0],
                    &mut scratch
                )
                .unwrap_err()
                .message,
            "Uncompressed length error in snappy"
        );
        assert_eq!(
            Codec::Snappy
                .decode(&[5, 0x0A, b'a', 0, 0, 0, 0], &mut scratch)
                .unwrap_err()
                .message,
            "Error uncompressing block with Snappy"
        );
    }

    #[test]
    fn deflate_is_raw_and_errors_are_reported() {
        let mut block = Vec::new();
        Codec::Deflate.encode(b"", &mut block).unwrap();
        assert_eq!(block, [0x03, 0x00]);
        let mut scratch = Vec::new();
        let stored = [0x01, 0x03, 0x00, 0xFC, 0xFF, b'a', b'b', b'c'];
        assert_eq!(
            Codec::Deflate.decode(&stored, &mut scratch).unwrap(),
            b"abc"
        );
        for corrupt in [&b""[..], &[0xFF, 0xFF, 0xFF], &stored[..6]] {
            assert_eq!(
                Codec::Deflate
                    .decode(corrupt, &mut scratch)
                    .unwrap_err()
                    .message,
                "Error decompressing block with deflate, possible data error"
            );
        }
    }

    #[test]
    fn zstandard_concatenated_frames_without_content_size() {
        let frame = |content: &[u8]| {
            let mut frame = vec![0x28, 0xB5, 0x2F, 0xFD, 0x00, 0x58];
            let header = ((content.len() as u32) << 3) | 1;
            frame.extend_from_slice(&header.to_le_bytes()[..3]);
            frame.extend_from_slice(content);
            frame
        };
        let mut block = frame(b"hello ");
        block.extend_from_slice(&[0x50, 0x2A, 0x4D, 0x18, 2, 0, 0, 0, 0xAA, 0xBB]);
        block.extend(frame(b"world"));
        let mut scratch = Vec::new();
        assert_eq!(
            Codec::Zstandard.decode(&block, &mut scratch).unwrap(),
            b"hello world"
        );
        assert!(
            Codec::Zstandard
                .decode(b"", &mut scratch)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn zstandard_concatenated_encoded_frames() {
        let mut block = Vec::new();
        let mut frame = Vec::new();
        let first = sample(300_000);
        let second = sample(1000);
        Codec::Zstandard.encode(&first, &mut frame).unwrap();
        block.extend_from_slice(&frame);
        Codec::Zstandard.encode(&second, &mut frame).unwrap();
        block.extend_from_slice(&frame);
        let mut scratch = Vec::new();
        let decoded = Codec::Zstandard.decode(&block, &mut scratch).unwrap();
        assert!(decoded[..first.len()] == first[..] && decoded[first.len()..] == second[..]);
    }

    #[test]
    fn corrupt_input_is_an_error() {
        let data = sample(100_000);
        let mut scratch = Vec::new();
        for codec in [
            Codec::Deflate,
            Codec::Snappy,
            Codec::Lzma,
            Codec::Zstandard,
            Codec::Bzip2,
            Codec::Xz,
        ] {
            let mut block = Vec::new();
            codec.encode(&data, &mut block).unwrap();
            let truncated = &block[..block.len() / 2];
            assert!(
                codec.decode(truncated, &mut scratch).is_err(),
                "{codec:?} truncated"
            );
            assert!(
                codec.decode(&[0x42; 64], &mut scratch).is_err(),
                "{codec:?} garbage"
            );
        }
    }

    #[test]
    fn mutated_blocks_are_errors_or_data() {
        let data = sample(20_000);
        let mut scratch = Vec::new();
        let mut state: u32 = 99;
        let mut next = || {
            state = state.wrapping_mul(1_103_515_245).wrapping_add(12345);
            (state >> 8) as usize
        };
        for codec in ALL {
            let mut block = Vec::new();
            codec.encode(&data, &mut block).unwrap();
            for _ in 0..200 {
                let mut mutated = block.clone();
                for _ in 0..1 + next() % 4 {
                    let at = next() % mutated.len();
                    mutated[at] = next() as u8;
                }
                mutated.truncate(1 + next() % mutated.len());
                let _ = codec.decode(&mutated, &mut scratch);
            }
        }
    }

    #[test]
    fn lzma_is_raw_lzma2() {
        let mut block = Vec::new();
        Codec::Lzma.encode(b"", &mut block).unwrap();
        assert_eq!(block, [0x00]);
        let uncompressed_chunk = [0x01, 0x00, 0x02, b'a', b'b', b'c', 0x00];
        let mut scratch = Vec::new();
        assert_eq!(
            Codec::Lzma
                .decode(&uncompressed_chunk, &mut scratch)
                .unwrap(),
            b"abc"
        );
        Codec::Lzma.encode(&sample(10_000), &mut block).unwrap();
        assert_eq!(block[0], 0xE0);
        assert_eq!(*block.last().unwrap(), 0x00);
    }
}
