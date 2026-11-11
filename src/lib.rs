//! `avro`: a reader and writer for Apache Avro object container files as a DuckDB extension on the C API v2.
//!
//! The extension exposes `read_avro` (a table function over one or more `.avro` files), `avro_metadata` and a
//! `COPY ... TO ... (FORMAT avro)` writer. It uses nothing but the C API v2 (`duckdb-ext-api`) and loads into the
//! DuckDB build named in `DUCKDB_PIN`.

// the Kani build (`make kani`) formats no error messages (`fmt_error!`), so a parameter that only feeds a message goes
// unused there
#![cfg_attr(kani, allow(unused_variables))]
// no `unsafe` in the extension itself: all of it is behind the C API wrapper (`duckdb-ext-api`). The proof harnesses
// (`cfg(kani)` only) place decoder trees in raw stack storage, so the ban holds for every build but that one.
#![cfg_attr(not(kani), forbid(unsafe_code))]

use duckdb_ext_api::{Context, Extension, Result};

/// `Error::<kind>(format!(..))` for the value conversions the proof harnesses (`verification`, `make kani`) check.
/// Under `cfg(kani)` the message is the bare format string: `core::fmt` is far beyond what CBMC explores in reasonable
/// time, and no harness checks message text; the error code and the control flow are what they prove.
#[cfg(not(kani))]
macro_rules! fmt_error {
    ($kind:ident, $($arg:tt)*) => {
        duckdb_ext_api::Error::$kind(format!($($arg)*))
    };
}
#[cfg(kani)]
macro_rules! fmt_error {
    ($kind:ident, $fmt:literal $(, $arg:expr)* $(,)?) => {{
        $(let _ = &$arg;)*
        duckdb_ext_api::Error::$kind($fmt)
    }};
}

mod copy;
mod metadata;
mod reader;
#[cfg(kani)]
mod verification;

duckdb_ext_api::duckdb_extension!(avro, avro_init_c_api_v2, "v0.1.0", load);

/// The log type of the entries the extension writes.
pub(crate) const LOG_TYPE: &str = "Avro";

fn load(extension: &Extension, context: &Context) -> Result<()> {
    reader::register(extension, context)?;
    metadata::register(extension, context)?;
    copy::register(extension, context)?;
    context.log(duckdb_ext_api::sys::DUCKDB_V2_LOG_LEVEL_DEBUG, LOG_TYPE, "avro loaded")?;
    Ok(())
}
