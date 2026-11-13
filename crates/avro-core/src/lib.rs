// no unsafe code, except that the proof harnesses (`cfg(kani)`, `make kani`) assemble their symbolic strings with
// `from_utf8_unchecked` (`kani_support`): a normal build forbids it outright
#![cfg_attr(not(kani), forbid(unsafe_code))]
#![cfg_attr(kani, deny(unsafe_code))]
// the Kani build (`make kani`) formats no error messages (`error::invalid_input!`), so a parameter that only feeds a
// message goes unused there
#![cfg_attr(kani, allow(unused_variables))]
//! Pure-Rust Apache Avro core: the schema model, the binary decoder/encoder and the object container reader/writer.
//!
//! Reading a file:
//!
//! ```no_run
//! use avro_core::{Decoder, FileReader, Value};
//!
//! let bytes = std::fs::read("data.avro")?;
//! let reader = FileReader::open(&bytes)?;
//! let schema = reader.schema();
//! // `reader.blocks()` is known up front; each block decompresses independently, e.g. one per thread
//! for block in reader.blocks() {
//!     let data = reader.decompress(block)?;
//!     let mut dec = Decoder::new(&data);
//!     for _ in 0..block.row_count {
//!         // the hot path walks the schema with the typed `Decoder::read_*` calls; `Value` is the convenience tree
//!         let row = Value::read(&mut dec, schema, schema.root())?;
//!         println!("{}", row.to_json(schema, schema.root()).to_string());
//!     }
//! }
//! # Ok::<(), avro_core::Error>(())
//! ```
//!
//! Writing a file: build a [`Schema`] (or parse one), create a [`FileWriter`], encode rows into an [`Encoder`] and
//! hand each finished batch to [`FileWriter::write_block`].

pub mod codec;
pub mod container;
pub mod crc32;
pub mod decode;
pub mod encode;
pub mod error;
pub mod json;
pub mod schema;
pub mod value;

pub use codec::Codec;
pub use container::{BlockInfo, FileReader, FileWriter, Header};
pub use decode::{BlockCount, Decoder};
pub use encode::Encoder;
pub use error::{Error, ErrorKind, Result};
pub use json::Json;
pub use schema::{Field, Name, Schema, SchemaBuilder, SchemaId, SchemaKind, SchemaNode};
pub use value::Value;

/// Symbolic inputs shared by the proof harnesses (`cargo kani -p avro-core`, `make kani`).
///
/// A symbolic `&str` is a symbolic byte array of symbolic length whose well-formedness is assumed through
/// [`well_formed_utf8`] (the byte-sequence table of the Unicode standard, a few table lookups per byte) and then
/// `from_utf8_unchecked`. Building the string char by char (`String::push` of a symbolic `char`) or validating it
/// with `str::from_utf8` (the word-at-a-time ASCII fast path on symbolic bytes) costs CBMC orders of magnitude more.
#[cfg(kani)]
#[allow(unsafe_code, dead_code)]
pub(crate) mod kani_support {
    /// A slice of symbolic bytes and symbolic length `0..=N`.
    pub(crate) fn any_slice<const N: usize>(buf: &[u8; N]) -> &[u8] {
        let len: usize = kani::any();
        kani::assume(len <= N);
        &buf[..len]
    }

    /// Well-formed UTF-8 by the byte-sequence table of the Unicode standard (Table 3-7), independently of `core::str`.
    pub(crate) fn well_formed_utf8(bytes: &[u8]) -> bool {
        let mut i = 0;
        while i < bytes.len() {
            let (len, lo, hi) = match bytes[i] {
                0x00..=0x7f => (1, 0x80, 0xbf),
                0xc2..=0xdf => (2, 0x80, 0xbf),
                0xe0 => (3, 0xa0, 0xbf),
                0xe1..=0xec | 0xee..=0xef => (3, 0x80, 0xbf),
                0xed => (3, 0x80, 0x9f),
                0xf0 => (4, 0x90, 0xbf),
                0xf1..=0xf3 => (4, 0x80, 0xbf),
                0xf4 => (4, 0x80, 0x8f),
                _ => return false,
            };
            if i + len > bytes.len() || (len > 1 && !(lo..=hi).contains(&bytes[i + 1])) {
                return false;
            }
            let mut k = 2;
            while k < len {
                if !(0x80..=0xbf).contains(&bytes[i + k]) {
                    return false;
                }
                k += 1;
            }
            i += len;
        }
        true
    }

    /// Every string of at most `N` bytes: `buf` is assumed well-formed UTF-8 up to a symbolic length.
    pub(crate) fn any_str<const N: usize>(buf: &[u8; N]) -> &str {
        let bytes = any_slice(buf);
        kani::assume(well_formed_utf8(bytes));
        // SAFETY: `well_formed_utf8` is the UTF-8 definition, assumed to hold for `bytes` just above
        unsafe { std::str::from_utf8_unchecked(bytes) }
    }

    /// Every ASCII string of at most `N` bytes.
    pub(crate) fn any_ascii_str<const N: usize>(buf: &[u8; N]) -> &str {
        let bytes = any_slice(buf);
        kani::assume(bytes.iter().all(u8::is_ascii));
        // SAFETY: ASCII bytes are UTF-8
        unsafe { std::str::from_utf8_unchecked(bytes) }
    }
}
