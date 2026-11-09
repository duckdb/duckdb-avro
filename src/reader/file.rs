//! Reading an Avro file into memory through the engine's file system, and the error mapping of `avro-core`.

use duckdb_ext_api::sys::{DUCKDB_V2_FILE_FLAG_EXTERNAL_FILE_CACHE, DUCKDB_V2_FILE_FLAG_READ};
use duckdb_ext_api::types::type_id;
use duckdb_ext_api::{Context, Error, FileOpenOptions, FileSystem, Result, Value};

/// The field of a file struct that holds the path.
const FILE_PATH_FIELD: &str = "filename";

/// Maps an `avro-core` error onto the engine's error classes: malformed input is an invalid-input error, an unsupported
/// feature a not-implemented one.
pub(crate) fn convert_error(error: avro_core::Error) -> Error {
    match error.kind() {
        avro_core::ErrorKind::InvalidInput => Error::invalid_input(error.message()),
        avro_core::ErrorKind::NotImplemented => Error::not_implemented(error.message()),
        avro_core::ErrorKind::Io => Error::io(error.message()),
    }
}

/// Reads the whole file at `path` with `options` (plus the READ and external-file-cache flags).
pub(crate) fn read_file(context: &Context, path: &str, options: &FileOpenOptions) -> Result<Vec<u8>> {
    options.set_flag(DUCKDB_V2_FILE_FLAG_READ)?.set_flag(DUCKDB_V2_FILE_FLAG_EXTERNAL_FILE_CACHE)?;
    let file = context.file_system()?.open(path, options)?;
    file.read_to_end()
}

/// Reads the whole file at `path` with default options.
pub(crate) fn read_path(context: &Context, path: &str) -> Result<Vec<u8>> {
    let options = context.file_system()?.open_options()?;
    read_file(context, path, &options)
}

/// The file a single-file function receives: a VARCHAR path, or a STRUCT holding the path in its `filename` field and
/// the options to open the file with in its other fields (e.g. the `file_size` a listing reported). Returns the path and
/// the open options with those values set.
pub(crate) fn file_argument(file_system: &FileSystem, file: &Value) -> Result<(String, FileOpenOptions)> {
    if file.is_null()? {
        return Err(Error::invalid_input("A file cannot be NULL"));
    }
    let options = file_system.open_options()?;
    let type_ = file.logical_type()?;
    match type_.id()? {
        type_id::VARCHAR => Ok((file.as_str()?, options)),
        type_id::STRUCT => {
            let mut path = None;
            let mut values = Vec::new();
            for i in 0..type_.param_count()? {
                let (name, _) = type_.param(i)?;
                let child = file.child(i)?;
                if name == FILE_PATH_FIELD {
                    if child.is_null()? || child.logical_type()?.id()? != type_id::VARCHAR {
                        break;
                    }
                    path = Some(child.as_str()?);
                } else if !child.is_null()? {
                    // a NULL option is an option that was not specified
                    values.push((name, child));
                }
            }
            let path = path.ok_or_else(|| {
                Error::invalid_input(
                    "A file struct must have a non-NULL VARCHAR \"filename\" field holding the path of the file",
                )
            })?;
            for (name, value) in &values {
                options.set_value(name, value)?;
            }
            Ok((path, options))
        }
        _ => Err(Error::invalid_input(format!(
            "A file must be a VARCHAR path or a STRUCT with a \"filename\" field, not {}",
            type_.to_text()?
        ))),
    }
}
