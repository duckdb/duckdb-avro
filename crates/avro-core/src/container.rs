//! The Avro object container file: header parsing, a block index for parallel decoding, and the block writer.
//!
//! Reading works over the whole file in memory (`&[u8]`). [`FileReader::open`] parses the header and walks the
//! block headers once (no decompression), so the block count is known up front and every [`BlockInfo`] can be
//! decompressed independently, on any thread, with [`FileReader::decompress`].

use std::io::Write;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::codec::Codec;
use crate::decode::Decoder;
use crate::encode::{write_bytes_to, write_long_to};
use crate::error::{Error, Result, invalid_input, message, not_implemented};
use crate::schema::Schema;

/// The four magic bytes every container file starts with.
pub const MAGIC: [u8; 4] = *b"Obj\x01";
/// The error text for a file that does not start with [`MAGIC`].
pub const BAD_MAGIC: &str = "Incorrect Avro container file magic number";
/// Metadata key of the schema.
pub const SCHEMA_KEY: &str = "avro.schema";
/// Metadata key of the codec.
pub const CODEC_KEY: &str = "avro.codec";

/// The parsed file header.
#[derive(Debug, Clone)]
pub struct Header {
    /// Every metadata entry in file order, values binary-safe (`avro.schema` and `avro.codec` included).
    pub metadata: Vec<(String, Vec<u8>)>,
    /// The writer's schema, parsed from `avro.schema`.
    pub schema: Schema,
    /// The block codec (`null` when `avro.codec` is absent).
    pub codec: Codec,
    /// The 16-byte sync marker that follows every block.
    pub sync_marker: [u8; 16],
    /// Length of the header in bytes; the first block starts here.
    pub len: usize,
}

impl Header {
    /// Parses the header at the start of `data`.
    pub fn parse(data: &[u8]) -> Result<Header> {
        if data.len() < 4 || data[..4] != MAGIC {
            return Err(Error::invalid_input(BAD_MAGIC));
        }
        let mut dec = Decoder::new(data);
        dec.seek(4);
        let mut metadata = Vec::new();
        loop {
            // a string key and a bytes value: two length prefixes at least
            let block = dec.read_block_count(2).map_err(|e| header_error(&e))?;
            if block.count == 0 {
                break;
            }
            for _ in 0..block.count {
                let key = dec
                    .read_string()
                    .map_err(|_| Error::invalid_input("Cannot read file header: metadata key is not valid UTF-8"))?;
                let value = dec.read_bytes().map_err(|e| header_error(&e))?;
                metadata.push((key.to_owned(), value.to_vec()));
            }
        }
        let sync = dec.read_raw(16, "the sync marker").map_err(|e| header_error(&e))?;
        let mut sync_marker = [0u8; 16];
        sync_marker.copy_from_slice(sync);
        let codec = match lookup(&metadata, CODEC_KEY) {
            None => Codec::Null,
            Some(bytes) => {
                let name = std::str::from_utf8(bytes)
                    .map_err(|_| Error::invalid_input("File header contains an unknown codec (not valid UTF-8)"))?;
                Codec::from_name(name)
                    .map_err(|_| not_implemented!("File header contains an unknown codec '{name}'"))?
            }
        };
        let schema_bytes = lookup(&metadata, SCHEMA_KEY)
            .ok_or_else(|| Error::invalid_input("File header doesn't contain a schema"))?;
        let schema_text = std::str::from_utf8(schema_bytes)
            .map_err(|_| Error::invalid_input("Cannot parse file header: the schema is not valid UTF-8"))?;
        let schema = Schema::parse(schema_text).map_err(|e| Error::with_context("Cannot parse file header", &e))?;
        Ok(Header { metadata, schema, codec, sync_marker, len: dec.position() })
    }

    /// The metadata value stored under `key` (the first one when it repeats).
    pub fn metadata_value(&self, key: &str) -> Option<&[u8]> {
        lookup(&self.metadata, key)
    }

    /// The schema JSON exactly as stored in the file.
    pub fn schema_json(&self) -> Option<&str> {
        self.metadata_value(SCHEMA_KEY).and_then(|b| std::str::from_utf8(b).ok())
    }
}

fn header_error(e: &Error) -> Error {
    Error::with_context("Cannot read file header", e)
}

fn lookup<'a>(metadata: &'a [(String, Vec<u8>)], key: &str) -> Option<&'a [u8]> {
    metadata.iter().find(|(k, _)| k == key).map(|(_, v)| v.as_slice())
}

/// One data block as found by the index pass: where its compressed bytes are and how many rows it holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockInfo {
    /// Position in the file's block sequence, from 0.
    pub index: usize,
    /// Rows (top-level values) in the block.
    pub row_count: u64,
    /// Offset of the compressed payload in the file.
    pub offset: usize,
    /// Length of the compressed payload in bytes.
    pub len: usize,
}

/// A container file held in memory, with its header parsed and its blocks indexed.
#[derive(Debug, Clone)]
pub struct FileReader<'a> {
    data: &'a [u8],
    header: Header,
    blocks: Vec<BlockInfo>,
}

impl<'a> FileReader<'a> {
    /// Parses the header and indexes every block (checking each block's sync marker; no decompression).
    pub fn open(data: &'a [u8]) -> Result<FileReader<'a>> {
        let header = Header::parse(data)?;
        let blocks = index_blocks(data, header.len, &header.sync_marker)?;
        Ok(FileReader { data, header, blocks })
    }
}

/// The block index pass of [`FileReader::open`]: walks the blocks from `start` (the end of the header), checking each
/// one's sync marker against `sync_marker`, without decompressing anything.
fn index_blocks(data: &[u8], start: usize, sync_marker: &[u8; 16]) -> Result<Vec<BlockInfo>> {
    // every block is at least two count bytes and the marker: the index never grows past this
    let mut blocks = Vec::with_capacity(data.len().saturating_sub(start) / 18);
    let mut dec = Decoder::new(data);
    dec.seek(start);
    while !dec.is_empty() {
        let index = blocks.len();
        let start = dec.position();
        let at = |what: &str| Error::invalid_input(message!("Avro block {index} at offset {start}: {what}"));
        let row_count = dec.read_long().map_err(|e| at(e.message()))?;
        let row_count = u64::try_from(row_count).map_err(|_| at(&message!("negative row count {row_count}")))?;
        let len = dec.read_long().map_err(|e| at(e.message()))?;
        let len = usize::try_from(len)
            .ok()
            .filter(|&len| dec.remaining() >= 16 && len <= dec.remaining() - 16)
            .ok_or_else(|| at(&message!("block size {len} exceeds the file")))?;
        let offset = dec.position();
        dec.read_raw(len, "a block")?;
        let sync = dec.read_raw(16, "the sync marker")?;
        if sync != sync_marker {
            return Err(at("sync marker mismatch"));
        }
        blocks.push(BlockInfo { index, row_count, offset, len });
    }
    Ok(blocks)
}

impl<'a> FileReader<'a> {
    pub fn header(&self) -> &Header {
        &self.header
    }

    pub fn schema(&self) -> &Schema {
        &self.header.schema
    }

    pub fn codec(&self) -> Codec {
        self.header.codec
    }

    /// The whole file.
    pub fn data(&self) -> &'a [u8] {
        self.data
    }

    /// Every block, in file order.
    pub fn blocks(&self) -> &[BlockInfo] {
        &self.blocks
    }

    /// Sum of the blocks' row counts.
    pub fn total_rows(&self) -> u64 {
        self.blocks.iter().map(|b| b.row_count).sum()
    }

    /// The compressed payload of a block.
    pub fn block_data(&self, block: &BlockInfo) -> &'a [u8] {
        &self.data[block.offset..block.offset + block.len]
    }

    /// Decompresses a block; the result holds `block.row_count` encoded values of the schema, back to back.
    pub fn decompress(&self, block: &BlockInfo) -> Result<Vec<u8>> {
        self.header
            .codec
            .decompress(self.block_data(block))
            .map_err(|e| Error::with_context(&message!("Avro block {} at offset {}", block.index, block.offset), &e))
    }
}

static SYNC_COUNTER: AtomicU64 = AtomicU64::new(0);

/// A random 16-byte sync marker from the clock, the stack address and a process counter through xorshift64*.
pub fn random_sync_marker() -> [u8; 16] {
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_nanos()).unwrap_or(0);
    let counter = SYNC_COUNTER.fetch_add(0x9E37_79B9_7F4A_7C15, Ordering::Relaxed);
    let local = 0u8;
    let address = std::ptr::addr_of!(local) as usize as u64;
    let mut state = (now as u64) ^ ((now >> 64) as u64).rotate_left(32) ^ address.rotate_left(17) ^ counter;
    if state == 0 {
        state = 0x2545_F491_4F6C_DD1D;
    }
    let mut out = [0u8; 16];
    for chunk in out.chunks_mut(8) {
        state ^= state >> 12;
        state ^= state << 25;
        state ^= state >> 27;
        let word = state.wrapping_mul(0x2545_F491_4F6C_DD1D);
        chunk.copy_from_slice(&word.to_le_bytes());
    }
    out
}

/// Writes an object container file block by block: the header on construction, then one block per
/// [`FileWriter::write_block`] call. The caller decides where blocks end.
#[derive(Debug)]
pub struct FileWriter<W: Write> {
    out: W,
    codec: Codec,
    sync_marker: [u8; 16],
    blocks_written: u64,
    rows_written: u64,
    bytes_written: u64,
}

impl<W: Write> FileWriter<W> {
    /// Writes the header: magic, `avro.codec`, `avro.schema`, the user metadata, and a random sync marker.
    /// User metadata keys may not start with `avro.` (reserved by the spec).
    pub fn new(out: W, schema: &Schema, codec: Codec, metadata: &[(String, Vec<u8>)]) -> Result<Self> {
        Self::with_schema_json(out, &schema.to_json(), codec, metadata)
    }

    /// Like [`FileWriter::new`] with the schema JSON text supplied by the caller.
    pub fn with_schema_json(out: W, schema_json: &str, codec: Codec, metadata: &[(String, Vec<u8>)]) -> Result<Self> {
        Self::with_sync_marker(out, schema_json, codec, metadata, random_sync_marker())
    }

    /// Like [`FileWriter::with_schema_json`] with the sync marker supplied by the caller (a deterministic file).
    pub fn with_sync_marker(
        out: W,
        schema_json: &str,
        codec: Codec,
        metadata: &[(String, Vec<u8>)],
        sync_marker: [u8; 16],
    ) -> Result<Self> {
        for (i, (key, _)) in metadata.iter().enumerate() {
            if key.starts_with("avro.") {
                return Err(invalid_input!("Avro metadata key '{key}' is reserved"));
            }
            if metadata[..i].iter().any(|(earlier, _)| earlier == key) {
                return Err(invalid_input!("duplicate Avro metadata key '{key}'"));
            }
        }
        let mut header = Vec::with_capacity(64 + schema_json.len());
        header.extend_from_slice(&MAGIC);
        write_long_to(&mut header, 2 + metadata.len() as i64);
        write_bytes_to(&mut header, CODEC_KEY.as_bytes());
        write_bytes_to(&mut header, codec.name().as_bytes());
        write_bytes_to(&mut header, SCHEMA_KEY.as_bytes());
        write_bytes_to(&mut header, schema_json.as_bytes());
        for (key, value) in metadata {
            write_bytes_to(&mut header, key.as_bytes());
            write_bytes_to(&mut header, value);
        }
        header.push(0);
        header.extend_from_slice(&sync_marker);
        let mut writer = FileWriter { out, codec, sync_marker, blocks_written: 0, rows_written: 0, bytes_written: 0 };
        writer.emit(&header)?;
        Ok(writer)
    }

    fn emit(&mut self, bytes: &[u8]) -> Result<()> {
        self.out.write_all(bytes)?;
        self.bytes_written += bytes.len() as u64;
        Ok(())
    }

    pub fn codec(&self) -> Codec {
        self.codec
    }

    pub fn sync_marker(&self) -> &[u8; 16] {
        &self.sync_marker
    }

    pub fn blocks_written(&self) -> u64 {
        self.blocks_written
    }

    pub fn rows_written(&self) -> u64 {
        self.rows_written
    }

    /// Bytes emitted to the sink so far.
    pub fn bytes_written(&self) -> u64 {
        self.bytes_written
    }

    /// Compresses `encoded_rows` (`row_count` values of the schema, encoded back to back) and writes the block
    /// `count, size, data, sync`. A block of zero rows is skipped.
    pub fn write_block(&mut self, row_count: u64, encoded_rows: &[u8]) -> Result<()> {
        if row_count == 0 {
            return Ok(());
        }
        let payload = self.codec.compress(encoded_rows)?;
        let mut prefix = Vec::with_capacity(20);
        write_long_to(&mut prefix, i64::try_from(row_count).map_err(|_| Error::invalid_input("row count overflow"))?);
        write_long_to(&mut prefix, payload.len() as i64);
        self.emit(&prefix)?;
        self.emit(&payload)?;
        let sync = self.sync_marker;
        self.emit(&sync)?;
        self.blocks_written += 1;
        self.rows_written += row_count;
        Ok(())
    }

    /// Flushes the sink and returns it.
    pub fn finish(mut self) -> Result<W> {
        self.out.flush()?;
        Ok(self.out)
    }

    pub fn get_ref(&self) -> &W {
        &self.out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encode::Encoder;
    use crate::schema::SchemaKind;

    #[test]
    fn header_only_file() {
        let schema = Schema::primitive(SchemaKind::Long);
        let meta = vec![("k".to_owned(), b"v\x00\xff".to_vec())];
        let bytes = FileWriter::new(Vec::new(), &schema, Codec::Deflate, &meta).unwrap().finish().unwrap();
        let reader = FileReader::open(&bytes).unwrap();
        assert_eq!(reader.blocks().len(), 0);
        assert_eq!(reader.codec(), Codec::Deflate);
        assert_eq!(reader.header().metadata_value("k"), Some(&b"v\x00\xff"[..]));
        assert_eq!(reader.header().metadata[0].0, CODEC_KEY);
        assert_eq!(reader.header().metadata[1].0, SCHEMA_KEY);
        assert_eq!(reader.header().schema_json(), Some(r#"{"type":"long"}"#));
    }

    #[test]
    fn bad_input() {
        assert_eq!(FileReader::open(b"Obj\x02xxxx").unwrap_err().message(), BAD_MAGIC);
        assert_eq!(FileReader::open(b"").unwrap_err().message(), BAD_MAGIC);
        assert!(FileReader::open(b"Obj\x01\x02\x02a\x02b").is_err());
        let schema = Schema::primitive(SchemaKind::Long);
        let mut writer = FileWriter::new(Vec::new(), &schema, Codec::Null, &[]).unwrap();
        let mut enc = Encoder::new();
        enc.write_long(7);
        writer.write_block(1, enc.as_bytes()).unwrap();
        let mut bytes = writer.finish().unwrap();
        assert_eq!(FileReader::open(&bytes).unwrap().total_rows(), 1);
        // truncated and corrupted trailers fail cleanly
        let full = bytes.clone();
        for cut in 1..full.len() {
            let _ = FileReader::open(&full[..cut]);
        }
        let last = bytes.len() - 1;
        bytes[last] ^= 1;
        assert!(FileReader::open(&bytes).unwrap_err().message().contains("sync marker"));
        let unknown = FileWriter::with_schema_json(Vec::new(), "\"int\"", Codec::Null, &[]).unwrap().finish().unwrap();
        let mut unknown = unknown;
        // patch the codec value "null" to "nope"
        let pos = unknown.windows(4).position(|w| w == b"null").unwrap();
        unknown[pos..pos + 4].copy_from_slice(b"nope");
        assert!(FileReader::open(&unknown).unwrap_err().message().contains("unknown codec 'nope'"));
    }
}
