//! A pure-Rust Avro layer that reads and writes object container files the way DuckDB's avro-c fork does: same
//! schema parsing rules and error messages, same schema JSON rendering, same file layout.

pub mod binary;
pub mod error;
pub mod schema;

pub use error::{AvroError, Result};
