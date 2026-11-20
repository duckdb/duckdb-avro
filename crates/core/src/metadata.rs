//! `avro_metadata(path)`, which lists the key-value metadata of a file.

use std::sync::Mutex;

use duckdb_neo::{
    Parameters, Result,
    bind_arguments::BindArgument,
    connection::Context,
    data_chunk::DataChunkRef,
    logical_type::{LogicalType, LogicalTypeID},
    signature::{FunctionParameter, SignatureBuilder},
    table_function::{
        BindFunctionHandle, ExecColumnInfo, InitColumnData, TableFunctionBuilder,
        TableFunctionCallbacks,
    },
};

use crate::avro::container::FileInfo;
use crate::error::binder;
use crate::file::{FileArgument, read_file};

#[derive(Debug)]
pub struct MetadataState {
    entries: Vec<(String, String)>,
    offset: Mutex<usize>,
}

#[derive(Debug)]
pub struct AvroMetadata;

impl TableFunctionCallbacks for AvroMetadata {
    type BindData = String;
    type LocalState = ();
    type GlobalState = MetadataState;

    fn bind(
        &self,
        context: &Context,
        arguments: Vec<BindArgument>,
        bind_handle: BindFunctionHandle<'_>,
    ) -> Result<String> {
        let varchar =
            LogicalType::create_from_id(context, LogicalTypeID::Varchar, Parameters::None)?;
        bind_handle.add_result_column("key", &varchar)?;
        bind_handle.add_result_column("value", &varchar)?;
        arguments
            .first()
            .and_then(|argument| argument.value.as_ref())
            .map(|value| value.get::<String>())
            .transpose()?
            .flatten()
            .ok_or_else(|| binder("avro_metadata requires a constant path"))
    }

    fn init_global_state(
        &self,
        context: &Context,
        path: &String,
        _column_data: InitColumnData<'_>,
    ) -> Result<Option<MetadataState>> {
        let file = FileArgument {
            path: path.clone(),
            options: Vec::new(),
        };
        let buffer = read_file(context, &file)?;
        let info = FileInfo::parse_header(&buffer)
            .map_err(|error| error.prefixed("Failed to read Avro file: "))?;
        let entries = info
            .metadata
            .into_iter()
            .map(|(key, value)| (key, String::from_utf8_lossy(&value).into_owned()))
            .collect();
        let state = MetadataState {
            entries,
            offset: Mutex::new(0),
        };
        Ok(Some(state))
    }

    fn exec(
        &self,
        _context: &Context,
        _path: &String,
        _local_state: Option<&mut ()>,
        global_state: Option<&MetadataState>,
        mut output: DataChunkRef<'_>,
        _column_info: ExecColumnInfo<'_>,
    ) -> Result<()> {
        let Some(state) = global_state else {
            return Ok(());
        };
        let mut offset = state
            .offset
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let count = (state.entries.len() - *offset).min(output.capacity()?);
        let rows = &state.entries[*offset..*offset + count];
        for (index, column) in (0..output.col_count()).zip([0, 1]) {
            let mut vector = output.get_vector_at_mut::<String>(index)?;
            vector.set_size(count)?;
            let mut writer = vector.writer()?;
            for (row, (key, value)) in rows.iter().enumerate() {
                writer.set(
                    row,
                    if column == 0 {
                        key.as_str()
                    } else {
                        value.as_str()
                    },
                )?;
            }
        }
        *offset += count;
        Ok(())
    }
}

/// `avro_metadata(path)`.
pub fn table_function() -> TableFunctionBuilder<AvroMetadata> {
    let signature = SignatureBuilder::new().param(FunctionParameter::new::<String>("path"));
    TableFunctionBuilder::new("avro_metadata", signature, AvroMetadata)
}
