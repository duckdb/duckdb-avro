//! `avro_metadata(path)`: the key-value metadata of an Avro object container file's header, in file order.

use std::sync::atomic::{AtomicUsize, Ordering};

use avro_core::Header;
use duckdb_ext_api::types::type_id;
use duckdb_ext_api::{
    BindInfo, Context, Error, Extension, LogicalType, Result, StringWriter, TableBind, TableBindInfo, TableExec,
    TableExecInfo, TableFunction, TableInitGlobal, TableInitGlobalInfo,
};

use crate::reader::{convert_error, read_path};

struct MetadataBind {
    path: String,
}

/// The metadata entries, and how many of them were output so far.
struct MetadataState {
    entries: Vec<(String, Vec<u8>)>,
    offset: AtomicUsize,
}

struct AvroMetadata;

impl TableBind for AvroMetadata {
    fn bind(bind: &BindInfo, info: &TableBindInfo, context: &Context) -> Result<()> {
        let varchar = LogicalType::new(context.factory()?, type_id::VARCHAR)?;
        info.add_result_column("key", &varchar)?;
        info.add_result_column("value", &varchar)?;
        bind.set_unique_bind_data(MetadataBind { path: bind.arg_value(0)?.as_str()? })
    }
}

impl TableInitGlobal for AvroMetadata {
    fn init_global(info: &TableInitGlobalInfo, context: &Context) -> Result<()> {
        let bind = info.bind_data::<MetadataBind>()?.ok_or_else(|| Error::internal("avro_metadata: no bind data"))?;
        let data = read_path(context, &bind.path)?;
        let header = Header::parse(&data).map_err(convert_error)?;
        info.set_global_state(MetadataState { entries: header.metadata, offset: AtomicUsize::new(0) })
    }
}

impl TableExec for AvroMetadata {
    fn exec(info: &TableExecInfo<'_>, _context: &Context) -> Result<()> {
        let state =
            info.global_state::<MetadataState>()?.ok_or_else(|| Error::internal("avro_metadata: no global state"))?;
        let output = info.output()?;
        let capacity = output.capacity()?;
        // claim the next rows: the state is shared by every thread of the scan
        let start = state.offset.fetch_add(capacity, Ordering::Relaxed).min(state.entries.len());
        let rows = &state.entries[start..(start + capacity).min(state.entries.len())];
        let keys = StringWriter::new(&output.vector(0)?, rows.len())?;
        let values = StringWriter::new(&output.vector(1)?, rows.len())?;
        for (row, (key, value)) in rows.iter().enumerate() {
            keys.write_str(row, key)?;
            values.write(row, value)?;
        }
        output.set_size(rows.len())
    }
}

/// Registers `avro_metadata`.
pub(crate) fn register(extension: &Extension, context: &Context) -> Result<()> {
    let factory = context.factory()?;
    let function = TableFunction::new(factory)?;
    function.set_name("avro_metadata")?;
    function.signature()?.add_parameter("path", &LogicalType::new(factory, type_id::VARCHAR)?)?;
    function.set_bind::<AvroMetadata>()?.set_init_global::<AvroMetadata>()?.set_exec::<AvroMetadata>()?;
    extension.register_table_function(&function)
}
