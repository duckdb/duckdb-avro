#![forbid(unsafe_code)]
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
