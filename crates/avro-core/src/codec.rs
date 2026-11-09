//! The block compression codecs of the object container format.

use std::io::{Read, Write};

use crate::crc32::crc32;
use crate::error::{Error, Result};

/// Largest decompressed block the codecs produce (2 GiB); a block claiming more is rejected as corrupt.
pub const MAX_DECOMPRESSED_BLOCK: usize = i32::MAX as usize;

/// A block codec. `name()` is the canonical lowercase spec name stored in the `avro.codec` header entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Codec {
    Null,
    /// Raw DEFLATE (no zlib header).
    Deflate,
    /// A raw snappy block followed by the big-endian CRC-32 of the uncompressed data.
    Snappy,
    Zstandard,
    Bzip2,
    /// The xz container (Avro spec name `xz`).
    Xz,
    /// avro-c's `lzma` codec: raw LZMA2 chunks without any container.
    Lzma,
}

impl Codec {
    /// All codecs this crate reads and writes.
    pub const ALL: [Codec; 7] =
        [Codec::Null, Codec::Deflate, Codec::Snappy, Codec::Zstandard, Codec::Bzip2, Codec::Xz, Codec::Lzma];

    /// Looks a codec up by name, case-insensitively; the error names the unknown codec.
    pub fn from_name(name: &str) -> Result<Codec> {
        let lower = name.to_ascii_lowercase();
        Codec::ALL
            .into_iter()
            .find(|c| c.name() == lower)
            .ok_or_else(|| Error::not_implemented(format!("Unknown codec '{name}'")))
    }

    /// The spec name (`null`, `deflate`, `snappy`, `zstandard`, `bzip2`, `xz`; `lzma` for the avro-c codec).
    pub fn name(self) -> &'static str {
        match self {
            Codec::Null => "null",
            Codec::Deflate => "deflate",
            Codec::Snappy => "snappy",
            Codec::Zstandard => "zstandard",
            Codec::Bzip2 => "bzip2",
            Codec::Xz => "xz",
            Codec::Lzma => "lzma",
        }
    }

    /// Decompresses one block's payload.
    pub fn decompress(self, data: &[u8]) -> Result<Vec<u8>> {
        let fail =
            |what: &str| Error::invalid_input(format!("Avro block failed to decompress with {}: {what}", self.name()));
        match self {
            Codec::Null => Ok(data.to_vec()),
            Codec::Deflate => miniz_oxide::inflate::decompress_to_vec_with_limit(data, MAX_DECOMPRESSED_BLOCK)
                .map_err(|e| fail(&format!("{:?}", e.status))),
            Codec::Snappy => {
                if data.len() < 4 {
                    return Err(fail("block shorter than the CRC-32 trailer"));
                }
                let (payload, trailer) = data.split_at(data.len() - 4);
                let expected = u32::from_be_bytes([trailer[0], trailer[1], trailer[2], trailer[3]]);
                let len = snap::raw::decompress_len(payload).map_err(|e| fail(&e.to_string()))?;
                if len > MAX_DECOMPRESSED_BLOCK {
                    return Err(fail("decompressed size too large"));
                }
                let out = snap::raw::Decoder::new().decompress_vec(payload).map_err(|e| fail(&e.to_string()))?;
                let actual = crc32(&out);
                if actual != expected {
                    return Err(fail(&format!("CRC-32 mismatch (stored {expected:08x}, computed {actual:08x})")));
                }
                Ok(out)
            }
            Codec::Zstandard => {
                let decoder = ruzstd::decoding::StreamingDecoder::new(data).map_err(|e| fail(&e.to_string()))?;
                read_capped(decoder, &fail)
            }
            Codec::Bzip2 => read_capped(bzip2::read::BzDecoder::new(data), &fail),
            Codec::Xz => {
                let mut out = CappedVec::default();
                lzma_rs::xz_decompress(&mut &*data, &mut out).map_err(|e| fail(&e.to_string()))?;
                Ok(out.0)
            }
            Codec::Lzma => {
                let mut out = CappedVec::default();
                lzma_rs::lzma2_decompress(&mut &*data, &mut out).map_err(|e| fail(&e.to_string()))?;
                Ok(out.0)
            }
        }
    }

    /// Compresses one block's payload.
    pub fn compress(self, data: &[u8]) -> Result<Vec<u8>> {
        let fail =
            |what: &str| Error::invalid_input(format!("Avro block failed to compress with {}: {what}", self.name()));
        match self {
            Codec::Null => Ok(data.to_vec()),
            // level 6, zlib's default (avro-c itself uses 9); raw deflate without the zlib header
            Codec::Deflate => Ok(miniz_oxide::deflate::compress_to_vec(data, 6)),
            Codec::Snappy => {
                let mut out = snap::raw::Encoder::new().compress_vec(data).map_err(|e| fail(&e.to_string()))?;
                out.extend_from_slice(&crc32(data).to_be_bytes());
                Ok(out)
            }
            // ruzstd 0.9 implements the Fastest (zstd level 1 class) encoder; Default/Better/Best are not implemented
            Codec::Zstandard => {
                Ok(ruzstd::encoding::compress_to_vec(data, ruzstd::encoding::CompressionLevel::Fastest))
            }
            Codec::Bzip2 => {
                let mut encoder = bzip2::write::BzEncoder::new(Vec::new(), bzip2::Compression::new(9));
                encoder.write_all(data).map_err(|e| fail(&e.to_string()))?;
                encoder.finish().map_err(|e| fail(&e.to_string()))
            }
            Codec::Xz => {
                let mut out = Vec::new();
                lzma_rs::xz_compress(&mut &*data, &mut out).map_err(|e| fail(&e.to_string()))?;
                Ok(out)
            }
            Codec::Lzma => {
                let mut out = Vec::new();
                lzma_rs::lzma2_compress(&mut &*data, &mut out).map_err(|e| fail(&e.to_string()))?;
                Ok(out)
            }
        }
    }
}

/// Reads a decoder to its end, rejecting output beyond [`MAX_DECOMPRESSED_BLOCK`].
fn read_capped(reader: impl Read, fail: &dyn Fn(&str) -> Error) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    reader.take(MAX_DECOMPRESSED_BLOCK as u64 + 1).read_to_end(&mut out).map_err(|e| fail(&e.to_string()))?;
    if out.len() > MAX_DECOMPRESSED_BLOCK {
        return Err(fail("decompressed size too large"));
    }
    Ok(out)
}

/// A `Write` sink that refuses to grow beyond [`MAX_DECOMPRESSED_BLOCK`].
#[derive(Default)]
struct CappedVec(Vec<u8>);

impl Write for CappedVec {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if self.0.len() + buf.len() > MAX_DECOMPRESSED_BLOCK {
            return Err(std::io::Error::other("decompressed size too large"));
        }
        self.0.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::Codec;

    #[test]
    fn names() {
        assert_eq!(Codec::from_name(" DEFLATE ").unwrap(), Codec::Deflate);
        assert_eq!(Codec::from_name("zstandard").unwrap().name(), "zstandard");
        assert!(Codec::from_name("nope").unwrap_err().message().contains("Unknown codec 'nope'"));
    }

    #[test]
    fn every_codec_round_trips() {
        let data: Vec<u8> = (0..50_000u32).map(|i| (i % 251) as u8).collect();
        for codec in Codec::ALL {
            let compressed = codec.compress(&data).unwrap();
            assert_eq!(codec.decompress(&compressed).unwrap(), data, "{}", codec.name());
            assert_eq!(codec.decompress(&codec.compress(&[]).unwrap()).unwrap(), Vec::<u8>::new(), "{}", codec.name());
        }
        // corrupt input is an error, not a panic
        for codec in Codec::ALL {
            let _ = codec.decompress(&[0xff, 0x00, 0x12, 0x34, 0x56]);
            let _ = codec.decompress(&[]);
        }
    }
}
