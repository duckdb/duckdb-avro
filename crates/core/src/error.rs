//! Constructors for the errors the extension raises, each with the error class DuckDB reports it under.

use duckdb_neo::{Error, error::DuckDBError};

use crate::avro::AvroError;

fn error(code: DuckDBError, message: impl Into<String>) -> Error {
    Error {
        code,
        message: message.into(),
    }
}

pub fn invalid_input(message: impl Into<String>) -> Error {
    error(DuckDBError::InputInvalid, message)
}

pub fn out_of_range(message: impl Into<String>) -> Error {
    error(DuckDBError::InputOutOfRange, message)
}

pub fn binder(message: impl Into<String>) -> Error {
    error(DuckDBError::QueryBinder, message)
}

pub fn not_implemented(message: impl Into<String>) -> Error {
    error(DuckDBError::QueryNotImplemented, message)
}

pub fn invalid_configuration(message: impl Into<String>) -> Error {
    error(DuckDBError::ConfigurationInvalid, message)
}

pub fn internal(message: impl Into<String>) -> Error {
    error(DuckDBError::RuntimeInternal, message)
}

impl From<AvroError> for Error {
    fn from(error: AvroError) -> Self {
        invalid_input(error.message)
    }
}
