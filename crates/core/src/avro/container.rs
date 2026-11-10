//! Object container files: header parsing, block indexing and writing.

use std::collections::HashMap;
use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use super::binary::{Decoder, write_bytes, write_long};
use super::codec::Codec;
use super::error::{AvroError, Result};
use super::schema::Schema;

pub const MAGIC: [u8; 4] = *b"Obj\x01";
pub const SYNC_SIZE: usize = 16;

const CODEC_KEY: &str = "avro.codec";
const SCHEMA_KEY: &str = "avro.schema";
const MAX_CODEC_NAME_LEN: usize = 10;

/// One block of an object container file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockInfo {
    /// Offset of the (compressed) block data in the file.
    pub offset: usize,
    /// Size of the (compressed) block data.
    pub size: usize,
    pub object_count: u64,
}

/// The header and block index of an object container file held in memory.
#[derive(Debug, Clone)]
pub struct FileInfo {
    pub schema: Schema,
    pub codec: Codec,
    /// The header metadata in file order (including avro.schema and avro.codec).
    pub metadata: Vec<(String, Vec<u8>)>,
    pub sync: [u8; 16],
    pub blocks: Vec<BlockInfo>,
}

impl FileInfo {
    /// Parses the header and indexes every block, like avro-c's `avro_file_reader_memory`.
    pub fn parse(data: &[u8]) -> Result<FileInfo> {
        let (mut info, mut decoder) = Self::parse_header_with_decoder(data)?;
        info.blocks = index_blocks(&mut decoder, MAGIC.len(), &info.sync)?;
        Ok(info)
    }

    /// Parses the header only; `blocks` is left empty.
    pub fn parse_header(data: &[u8]) -> Result<FileInfo> {
        Self::parse_header_with_decoder(data).map(|(info, _)| info)
    }

    fn parse_header_with_decoder(data: &[u8]) -> Result<(FileInfo, Decoder<'_>)> {
        if data.get(..MAGIC.len()) != Some(&MAGIC[..]) {
            return Err(AvroError::new("Incorrect Avro container file magic number"));
        }
        let mut decoder = Decoder::new(&data[MAGIC.len()..]);
        let metadata =
            read_metadata(&mut decoder).map_err(|e| e.prefixed("Cannot read file header: "))?;

        let codec = match metadata_value(&metadata, CODEC_KEY) {
            None => Codec::Null,
            Some(value) => {
                let name = &value[..value.len().min(MAX_CODEC_NAME_LEN)];
                let name = name.split(|&b| b == 0).next().unwrap_or_default();
                std::str::from_utf8(name)
                    .ok()
                    .and_then(|name| Codec::from_name(name).ok())
                    .ok_or_else(|| AvroError::new("File header contains an unknown codec"))?
            }
        };
        let schema_json = metadata_value(&metadata, SCHEMA_KEY)
            .ok_or_else(|| AvroError::new("File header doesn't contain a schema"))?;
        let schema =
            Schema::parse(schema_json).map_err(|e| e.prefixed("Cannot parse file header: "))?;

        let mut sync = [0u8; SYNC_SIZE];
        sync.copy_from_slice(decoder.read_fixed(SYNC_SIZE)?);

        let info = FileInfo {
            schema,
            codec,
            metadata,
            sync,
            blocks: Vec::new(),
        };
        Ok((info, decoder))
    }

    /// Decompresses a block (as indexed by `parse`) of `data`, the same buffer passed to `parse`.
    pub fn decode_block<'a>(
        &self,
        data: &'a [u8],
        block: &BlockInfo,
        scratch: &'a mut Vec<u8>,
    ) -> Result<&'a [u8]> {
        let prefix = "Cannot decode Avro file block: ";
        let compressed = block
            .offset
            .checked_add(block.size)
            .and_then(|end| data.get(block.offset..end))
            .ok_or_else(|| {
                AvroError::new("Avro file block exceeds memory buffer").prefixed(prefix)
            })?;
        self.codec
            .decode(compressed, scratch)
            .map_err(|e| e.prefixed(prefix))
    }
}

fn metadata_value<'m>(metadata: &'m [(String, Vec<u8>)], key: &str) -> Option<&'m [u8]> {
    metadata
        .iter()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v.as_slice())
}

fn read_metadata(decoder: &mut Decoder) -> Result<Vec<(String, Vec<u8>)>> {
    let mut metadata: Vec<(String, Vec<u8>)> = Vec::new();
    let mut positions: HashMap<String, usize> = HashMap::new();
    loop {
        let count = decoder
            .read_long()
            .map_err(|e| e.prefixed("Cannot read map block count: "))?;
        if count == 0 {
            return Ok(metadata);
        }
        if count < 0 {
            decoder
                .read_long()
                .map_err(|e| e.prefixed("Cannot read map block size: "))?;
        }
        for _ in 0..count.unsigned_abs() {
            let key = read_key(decoder).map_err(|e| e.prefixed("Cannot read map key: "))?;
            let value = decoder
                .read_bytes()
                .map_err(|e| e.prefixed("Cannot read bytes value: "))?
                .to_vec();
            match positions.get(&key) {
                Some(&position) => metadata[position].1 = value,
                None => {
                    positions.insert(key.clone(), metadata.len());
                    metadata.push((key, value));
                }
            }
        }
    }
}

fn read_key(decoder: &mut Decoder) -> Result<String> {
    let len = decoder
        .read_long()
        .map_err(|e| e.prefixed("Cannot read string length: "))?;
    let len = usize::try_from(len).map_err(|_| AvroError::new("Invalid string value length"))?;
    let bytes = decoder.read_fixed(len)?;
    // Keys are C strings in avro-c, so they end at the first NUL.
    let bytes = bytes.split(|&b| b == 0).next().unwrap_or_default();
    Ok(String::from_utf8_lossy(bytes).into_owned())
}

fn index_blocks(
    decoder: &mut Decoder,
    base_offset: usize,
    sync: &[u8; SYNC_SIZE],
) -> Result<Vec<BlockInfo>> {
    let mut blocks = Vec::new();
    while !decoder.is_empty() {
        let count = decoder
            .read_long()
            .map_err(|e| e.prefixed("Cannot index file block count: "))?;
        let size = decoder
            .read_long()
            .map_err(|e| e.prefixed("Cannot index file block size: "))?;
        if count <= 0 || size < 0 {
            return Err(AvroError::new("Invalid Avro file block count or size"));
        }
        let size = usize::try_from(size)
            .ok()
            .filter(|&size| size <= decoder.remaining())
            .ok_or_else(|| AvroError::new("Avro file block exceeds memory buffer"))?;
        let offset = base_offset + decoder.position();
        decoder.skip(size)?;
        let block_sync = decoder
            .read_fixed(SYNC_SIZE)
            .map_err(|e| e.prefixed("Cannot index file block sync: "))?;
        if block_sync != sync {
            return Err(AvroError::new("Incorrect sync bytes"));
        }
        blocks.push(BlockInfo {
            offset,
            size,
            object_count: count as u64,
        });
    }
    Ok(blocks)
}

/// The bytes of a file header: magic, metadata (avro.codec, avro.schema, then `metadata` in order), 0, sync.
pub fn write_header(
    out: &mut Vec<u8>,
    schema_json: &str,
    codec: Codec,
    metadata: &[(String, String)],
    sync: &[u8; 16],
) {
    out.extend_from_slice(&MAGIC);
    write_long(out, metadata.len() as i64 + 2);
    write_bytes(out, CODEC_KEY.as_bytes());
    write_bytes(out, codec.name().as_bytes());
    write_bytes(out, SCHEMA_KEY.as_bytes());
    write_bytes(out, schema_json.as_bytes());
    for (key, value) in metadata {
        write_bytes(out, key.as_bytes());
        write_bytes(out, value.as_bytes());
    }
    write_long(out, 0);
    out.extend_from_slice(sync);
}

/// Appends one block (count, size, compressed data, sync) holding `object_count` encoded objects.
pub fn write_block(
    out: &mut Vec<u8>,
    object_count: u64,
    data: &[u8],
    codec: Codec,
    sync: &[u8; 16],
    scratch: &mut Vec<u8>,
) -> Result<()> {
    let count = i64::try_from(object_count)
        .map_err(|_| AvroError::new("Avro file block object count too large"))?;
    let compressed = if codec == Codec::Null {
        data
    } else {
        codec.encode(data, scratch)?;
        scratch.as_slice()
    };
    write_long(out, count);
    write_long(out, compressed.len() as i64);
    out.extend_from_slice(compressed);
    out.extend_from_slice(sync);
    Ok(())
}

/// A sync marker that differs between files.
pub fn generate_sync() -> [u8; 16] {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let counter = COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    let half = |salt: u8| {
        let mut hasher = RandomState::new().build_hasher();
        hasher.write_u128(nanos);
        hasher.write_u64(counter);
        hasher.write_u32(std::process::id());
        hasher.write_u8(salt);
        hasher.finish().to_le_bytes()
    };
    let mut sync = [0u8; SYNC_SIZE];
    sync[..8].copy_from_slice(&half(0));
    sync[8..].copy_from_slice(&half(1));
    sync
}

#[cfg(test)]
mod tests {
    use super::*;

    const SCHEMA: &str =
        r#"{"type":"record","name":"root","fields":[{"name":"a","type":["null",{"type":"int"}]}]}"#;
    const SYNC: [u8; 16] = *b"0123456789abcdef";

    fn header(metadata: &[u8]) -> Vec<u8> {
        let mut out = MAGIC.to_vec();
        out.extend_from_slice(metadata);
        out.extend_from_slice(&SYNC);
        out
    }

    fn error(data: &[u8]) -> String {
        FileInfo::parse(data).unwrap_err().message
    }

    fn entry(out: &mut Vec<u8>, key: &[u8], value: &[u8]) {
        write_bytes(out, key);
        write_bytes(out, value);
    }

    #[test]
    fn header_size_matches_avro_c() {
        let mut out = Vec::new();
        write_header(&mut out, SCHEMA, Codec::Null, &[], &SYNC);
        assert_eq!(out.len(), 138);
    }

    #[test]
    fn magic() {
        for data in [&b""[..], b"Ob", b"Obj\x02rest", b"PAR1"] {
            assert_eq!(error(data), "Incorrect Avro container file magic number");
        }
    }

    #[test]
    fn header_errors() {
        assert_eq!(
            error(b"Obj\x01"),
            "Cannot read file header: Cannot read map block count: Cannot read 1 bytes from memory buffer"
        );
        let mut meta = Vec::new();
        write_long(&mut meta, 1);
        write_bytes(&mut meta, b"avro.schema");
        meta.push(0x80);
        let mut data = MAGIC.to_vec();
        data.extend_from_slice(&meta);
        assert_eq!(
            error(&data),
            "Cannot read file header: Cannot read bytes value: Cannot read bytes length: Cannot read 1 bytes from memory buffer"
        );
        let mut meta = Vec::new();
        write_long(&mut meta, 1);
        write_long(&mut meta, 20);
        assert_eq!(
            error(&header(&meta)),
            "Cannot read file header: Cannot read map key: Cannot read 20 bytes from memory buffer"
        );
        let mut meta = Vec::new();
        write_long(&mut meta, 0);
        assert_eq!(
            error(&header(&meta)),
            "File header doesn't contain a schema"
        );

        let mut meta = Vec::new();
        write_long(&mut meta, 1);
        entry(&mut meta, b"avro.schema", b"\"int\"");
        write_long(&mut meta, 0);
        let mut data = MAGIC.to_vec();
        data.extend_from_slice(&meta);
        data.extend_from_slice(&SYNC[..10]);
        assert_eq!(error(&data), "Cannot read 16 bytes from memory buffer");
    }

    #[test]
    fn schema_and_codec_errors() {
        let mut meta = Vec::new();
        write_long(&mut meta, 1);
        entry(&mut meta, b"avro.schema", b"\"nope\"");
        write_long(&mut meta, 0);
        assert_eq!(
            error(&header(&meta)),
            "Cannot parse file header: Unknown Avro \"type\": nope"
        );
        let mut meta = Vec::new();
        write_long(&mut meta, 2);
        entry(&mut meta, b"avro.schema", b"\"int\"");
        entry(&mut meta, b"avro.codec", b"brotli");
        write_long(&mut meta, 0);
        assert_eq!(
            error(&header(&meta)),
            "File header contains an unknown codec"
        );
    }

    #[test]
    fn codec_name_is_truncated() {
        for (raw, codec) in [
            (&b"zstandard\0junk"[..], Codec::Zstandard),
            (b"deflate", Codec::Deflate),
            (b"null\0", Codec::Null),
        ] {
            let mut meta = Vec::new();
            write_long(&mut meta, 2);
            entry(&mut meta, b"avro.codec", raw);
            entry(&mut meta, b"avro.schema", b"\"int\"");
            write_long(&mut meta, 0);
            assert_eq!(FileInfo::parse(&header(&meta)).unwrap().codec, codec);
        }
        let mut meta = Vec::new();
        write_long(&mut meta, 2);
        entry(&mut meta, b"avro.codec", b"zstandardx");
        entry(&mut meta, b"avro.schema", b"\"int\"");
        write_long(&mut meta, 0);
        assert_eq!(
            error(&header(&meta)),
            "File header contains an unknown codec"
        );
        let mut meta = Vec::new();
        write_long(&mut meta, 2);
        entry(&mut meta, b"avro.codec", b"snappy\0\0\0\0\0junk");
        entry(&mut meta, b"avro.schema", b"\"int\"");
        write_long(&mut meta, 0);
        assert_eq!(
            FileInfo::parse(&header(&meta)).unwrap().codec,
            Codec::Snappy
        );
    }

    #[test]
    fn metadata_blocks_duplicates_and_nul_keys() {
        let mut meta = Vec::new();
        let mut first = Vec::new();
        entry(&mut first, b"a", b"1");
        entry(&mut first, b"avro.schema", b"\"long\"");
        write_long(&mut meta, -2);
        write_long(&mut meta, first.len() as i64);
        meta.extend_from_slice(&first);
        write_long(&mut meta, 2);
        entry(&mut meta, b"b\0ignored", b"2");
        entry(&mut meta, b"a", b"3");
        write_long(&mut meta, 0);
        let info = FileInfo::parse(&header(&meta)).unwrap();
        assert_eq!(
            info.metadata,
            vec![
                ("a".to_owned(), b"3".to_vec()),
                ("avro.schema".to_owned(), b"\"long\"".to_vec()),
                ("b".to_owned(), b"2".to_vec()),
            ]
        );
        assert_eq!(info.codec, Codec::Null);
        assert_eq!(info.sync, SYNC);
        assert!(info.blocks.is_empty());
    }

    fn file_with_blocks(blocks: &[(i64, &[u8])]) -> Vec<u8> {
        let mut out = Vec::new();
        write_header(&mut out, "\"int\"", Codec::Null, &[], &SYNC);
        for (count, data) in blocks {
            write_long(&mut out, *count);
            write_long(&mut out, data.len() as i64);
            out.extend_from_slice(data);
            out.extend_from_slice(&SYNC);
        }
        out
    }

    #[test]
    fn block_index_errors() {
        let mut data = file_with_blocks(&[(1, b"\x02")]);
        data.push(0x80);
        assert_eq!(
            error(&data),
            "Cannot index file block count: Cannot read 1 bytes from memory buffer"
        );
        let mut data = file_with_blocks(&[]);
        data.push(0x02);
        assert_eq!(
            error(&data),
            "Cannot index file block size: Cannot read 1 bytes from memory buffer"
        );
        assert_eq!(
            error(&file_with_blocks(&[(0, b"")])),
            "Invalid Avro file block count or size"
        );
        let mut data = file_with_blocks(&[]);
        write_long(&mut data, 1);
        write_long(&mut data, -1);
        assert_eq!(error(&data), "Invalid Avro file block count or size");
        let mut data = file_with_blocks(&[]);
        write_long(&mut data, 1);
        write_long(&mut data, 100);
        data.extend_from_slice(&[0; 99]);
        assert_eq!(error(&data), "Avro file block exceeds memory buffer");
        let mut data = file_with_blocks(&[(1, b"\x02")]);
        data.truncate(data.len() - 1);
        assert_eq!(
            error(&data),
            "Cannot index file block sync: Cannot read 16 bytes from memory buffer"
        );
        let mut data = file_with_blocks(&[(1, b"\x02")]);
        let last = data.len() - 1;
        data[last] = b'X';
        assert_eq!(error(&data), "Incorrect sync bytes");
    }

    #[test]
    fn blocks_are_indexed() {
        let data = file_with_blocks(&[(1, b"\x02"), (3, b"\x04\x06\x08"), (2, b"")]);
        let info = FileInfo::parse(&data).unwrap();
        let header_len = data.len() - 3 * (2 + SYNC_SIZE) - 4;
        assert_eq!(
            info.blocks,
            vec![
                BlockInfo {
                    offset: header_len + 2,
                    size: 1,
                    object_count: 1
                },
                BlockInfo {
                    offset: header_len + 2 + 1 + SYNC_SIZE + 2,
                    size: 3,
                    object_count: 3
                },
                BlockInfo {
                    offset: data.len() - SYNC_SIZE,
                    size: 0,
                    object_count: 2
                },
            ]
        );
        let mut scratch = Vec::new();
        assert_eq!(
            info.decode_block(&data, &info.blocks[1], &mut scratch)
                .unwrap(),
            b"\x04\x06\x08"
        );
        let bogus = BlockInfo {
            offset: data.len(),
            size: 1,
            object_count: 1,
        };
        assert_eq!(
            info.decode_block(&data, &bogus, &mut scratch)
                .unwrap_err()
                .message,
            "Cannot decode Avro file block: Avro file block exceeds memory buffer"
        );
    }

    #[test]
    fn decode_block_errors_are_prefixed() {
        let mut data = Vec::new();
        write_header(&mut data, "\"int\"", Codec::Snappy, &[], &SYNC);
        write_block(&mut data, 1, b"", Codec::Null, &SYNC, &mut Vec::new()).unwrap();
        let info = FileInfo::parse(&data).unwrap();
        assert_eq!(
            info.decode_block(&data, &info.blocks[0], &mut Vec::new())
                .unwrap_err()
                .message,
            "Cannot decode Avro file block: Uncompressed length error in snappy"
        );
    }

    #[test]
    fn write_block_layout() {
        let mut out = Vec::new();
        let mut scratch = Vec::new();
        write_block(&mut out, 2, b"\x02\x04", Codec::Null, &SYNC, &mut scratch).unwrap();
        let mut expected = vec![0x04, 0x04, 0x02, 0x04];
        expected.extend_from_slice(&SYNC);
        assert_eq!(out, expected);
        assert_eq!(
            write_block(&mut out, u64::MAX, b"", Codec::Null, &SYNC, &mut scratch)
                .unwrap_err()
                .message,
            "Avro file block object count too large"
        );
    }

    #[test]
    fn header_layout() {
        let mut out = Vec::new();
        let metadata = [("k".to_owned(), "v".to_owned())];
        write_header(&mut out, "\"int\"", Codec::Deflate, &metadata, &SYNC);
        let mut expected = MAGIC.to_vec();
        write_long(&mut expected, 3);
        entry(&mut expected, b"avro.codec", b"deflate");
        entry(&mut expected, b"avro.schema", b"\"int\"");
        entry(&mut expected, b"k", b"v");
        write_long(&mut expected, 0);
        expected.extend_from_slice(&SYNC);
        assert_eq!(out, expected);
    }

    #[test]
    fn sync_markers_differ() {
        let a = generate_sync();
        let b = generate_sync();
        assert_ne!(a, b);
        assert_ne!(a, [0; 16]);
    }
}
