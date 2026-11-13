//! Reading files through DuckDB's file system.

use duckdb_neo::{
    Result,
    connection::Context,
    file::{FileBuilder, FileSystem},
    logical_type::LogicalTypeID,
    value::Value,
};

use crate::error::invalid_input;

const FILE_PATH_FIELD: &str = "filename";

/// A file as table functions that read files take it: a VARCHAR path, or a STRUCT holding the path in its `filename`
/// field and the options to open the file with in its other fields.
#[derive(Debug)]
pub struct FileArgument {
    pub path: String,
    pub options: Vec<(String, Value)>,
}

impl FileArgument {
    pub fn parse(file: &Value) -> Result<FileArgument> {
        if file.is_null()? {
            return Err(invalid_input("A file cannot be NULL"));
        }
        let file_type = file.logical_type()?;
        match file_type.id()? {
            LogicalTypeID::Varchar => {
                return Ok(FileArgument {
                    path: file.get::<String>()?.unwrap_or_default(),
                    options: Vec::new(),
                });
            }
            LogicalTypeID::Struct => {}
            _ => {
                return Err(invalid_input(format!(
                    "A file must be a VARCHAR path or a STRUCT with a \"filename\" field, not {}",
                    file_type.to_text()?
                )));
            }
        }
        let mut path = None;
        let mut options = Vec::new();
        for (index, (name, _)) in file_type.get_params()?.into_iter().enumerate() {
            let child = file.get_child(index)?;
            if name == FILE_PATH_FIELD {
                if path.is_none()
                    && !child.is_null()?
                    && child.logical_type()?.id()? == LogicalTypeID::Varchar
                {
                    path = child.get::<String>()?;
                }
                continue;
            }
            // A NULL option is an option that was not specified
            if !child.is_null()? {
                options.push((name, child));
            }
        }
        let path = path.ok_or_else(|| {
            invalid_input(
                "A file struct must have a non-NULL VARCHAR \"filename\" field holding the path of the file",
            )
        })?;
        Ok(FileArgument { path, options })
    }
}

/// Reads the whole file into memory, through the external file cache.
pub fn read_file(context: &Context, file: &FileArgument) -> Result<Vec<u8>> {
    let fs = FileSystem::new(context)?;
    let mut builder = FileBuilder::new(&fs, &file.path)?
        .read()
        .external_file_cache();
    for (name, value) in &file.options {
        builder = builder.set_value(name, value)?;
    }
    let handle = builder.open()?;
    let size =
        usize::try_from(handle.size()?).map_err(|_| invalid_input("Avro file is too large"))?;
    let mut buffer = vec![0; size];
    if size > 0 {
        handle.read_exact_at(0, &mut buffer)?;
    }
    Ok(buffer)
}
