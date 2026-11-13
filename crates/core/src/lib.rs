//! The DuckDB Avro extension: `read_avro`, `avro_metadata` and `COPY ... TO (FORMAT AVRO)`.

use duckdb_neo::{Result, extension::Extension};

pub mod avro;
pub mod copy;
pub mod decode;
pub mod error;
pub mod field_ids;
pub mod file;
pub mod metadata;
pub mod reader;
pub mod types;

/// Registers every function of the extension through `$link`, an `Extension` or a `Connection`.
#[macro_export]
macro_rules! register_functions {
    ($link:expr) => {{
        let link = $link;
        $crate::reader::single_file_function().register(link)?;
        $crate::reader::multi_file_function().register(link)?;
        $crate::metadata::table_function().register(link)?;
        $crate::copy::copy_function().register(link)?;
        duckdb_neo::Result::Ok(())
    }};
}

/// Registers everything the extension provides.
pub fn register(extension: &Extension) -> Result<()> {
    register_functions!(extension)
}
