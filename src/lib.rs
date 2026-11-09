//! `avro`: a reader and writer for Apache Avro object container files as a DuckDB extension on the C API v2.
//!
//! The extension exposes `read_avro` (a table function over one or more `.avro` files), `avro_metadata` and a
//! `COPY ... TO ... (FORMAT avro)` writer. It uses nothing but the C API v2 (`duckdb-ext-api`) and loads into the
//! DuckDB build named in `DUCKDB_PIN`.

use duckdb_ext_api::{Context, Extension, Result};

duckdb_ext_api::duckdb_extension!(avro, avro_init_c_api_v2, "v0.1.0", load);

/// The log type of the entries the extension writes.
pub(crate) const LOG_TYPE: &str = "Avro";

fn load(_extension: &Extension, context: &Context) -> Result<()> {
    // TODO: register read_avro, avro_metadata and the COPY TO writer.
    context.log(duckdb_ext_api::sys::DUCKDB_V2_LOG_LEVEL_DEBUG, LOG_TYPE, "avro loaded")?;
    Ok(())
}
