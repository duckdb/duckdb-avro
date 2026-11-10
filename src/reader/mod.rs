//! `read_single_avro_file(file)` and `read_avro(files, ...)`: scanning Avro object container files.
//!
//! `read_single_avro_file` reads one file, given as a path or as a file struct (`{'filename': ..., <open options>}`),
//! into memory and scans its blocks in parallel: `init_global` allows one thread per block, `claim_batch` hands out the
//! next block index, and `exec` decodes the claimed block into output chunks (a block larger than a chunk spans several
//! calls; an empty chunk ends the block). `get_bind_info` describes the file to the multi-file wrapper: the field ids of
//! its columns and nested fields, and every header metadata entry as an option. `read_avro` is the multi-file wrapper
//! around it, which adds globbing, file lists, `union_by_name`, `filename`, hive partitioning and `schema=`.

mod file;
mod node;
mod schema;

/// The pure value conversions the proof harnesses of `crate::verification` check.
#[cfg(kani)]
pub(crate) use node::{
    duration_to_interval, parse_uuid, read_decimal, time_micros_checked, time_millis_to_micros,
    timestamp_millis_to_micros, uuid_from_bytes,
};
#[cfg(kani)]
pub(crate) use schema::{decimal_annotation, decimal_width, enum_width, union_member_name};

use std::sync::atomic::{AtomicUsize, Ordering};

use avro_core::{BlockInfo, Decoder, FileReader, Header, Schema, SchemaKind};
use duckdb_ext_api::types::type_id;
use duckdb_ext_api::{
    BindInfo, Context, Error, Extension, LogicalType, MultiFileFunction, Result, TableBind, TableBindInfo,
    TableClaimBatch, TableClaimBatchInfo, TableExec, TableExecInfo, TableFunction, TableGetBindInfo,
    TableGetBindInfoInfo, TableInitGlobal, TableInitGlobalInfo, TableInitLocal, TableInitLocalInfo, Value,
};

pub(crate) use file::{convert_error, read_path};
use node::Node;
use schema::{Column, is_null_type};

/// The name of the single-file function.
const SINGLE_FILE_FUNCTION: &str = "read_single_avro_file";
/// The name of the multi-file function.
const MULTI_FILE_FUNCTION: &str = "read_avro";
/// How the files are called in the engine's messages.
const READER_TYPE: &str = "Avro";

/// An Avro object container file read into memory: the bind data of a scan.
pub(crate) struct AvroFile {
    data: Vec<u8>,
    header: Header,
    blocks: Vec<BlockInfo>,
    /// The columns the file is read as.
    columns: Vec<Column>,
    /// Whether the root is a record whose fields are the columns (else the one column reads the root value).
    root_is_struct: bool,
}

impl AvroFile {
    /// Reads and indexes the file.
    fn open(context: &Context, data: Vec<u8>) -> Result<AvroFile> {
        let (header, blocks) = {
            let reader = FileReader::open(&data).map_err(convert_error)?;
            (reader.header().clone(), reader.blocks().to_vec())
        };
        let root = schema::root_columns(context.factory()?, &header.schema)?;
        Ok(AvroFile { data, header, blocks, columns: root.columns, root_is_struct: root.root_is_struct })
    }

    fn schema(&self) -> &Schema {
        &self.header.schema
    }

    /// The decompressed rows of a block.
    fn decompress(&self, block: &BlockInfo) -> Result<Vec<u8>> {
        self.header.codec.decompress(&self.data[block.offset..block.offset + block.len]).map_err(|e| {
            convert_error(avro_core::Error::new(
                e.kind(),
                format!("Avro block {} at offset {}: {e}", block.index, block.offset),
            ))
        })
    }
}

/// The state every thread of a scan shares: the next block to hand out.
struct GlobalState {
    next_block: AtomicUsize,
}

/// The block a thread is reading.
struct CurrentBlock {
    /// The decompressed rows.
    data: Vec<u8>,
    /// The offset of the next row in `data`.
    position: usize,
    rows_left: u64,
}

/// A thread's scan: the block it claimed and the decoders of the columns it reads.
struct LocalState {
    /// The index of the claimed block, until `exec` decompresses it.
    claimed: Option<usize>,
    block: Option<CurrentBlock>,
    /// The decoders of the output columns, in output order.
    nodes: Vec<Node>,
    /// For a record root: the output column of each field of the root record, `None` when the field is not read.
    projection: Vec<Option<usize>>,
}

/// Decodes one row (one top-level value) into the output `nodes`; `projection` maps the fields of a record root onto
/// them.
fn decode_row(dec: &mut Decoder<'_>, file: &AvroFile, nodes: &mut [Node], projection: &[Option<usize>]) -> Result<()> {
    let schema = file.schema();
    if !file.root_is_struct {
        return nodes[0].decode(dec, schema);
    }
    // the root record, possibly wrapped in a union with null (a NULL root value is a row of NULLs)
    let mut id = schema.resolve(schema.root());
    loop {
        match &schema[id].kind {
            SchemaKind::Union { branches } => {
                let branch = dec.read_union_index(branches.len()).map_err(convert_error)?;
                let chosen = branches[branch];
                if is_null_type(schema, chosen) {
                    dec.skip(schema, chosen).map_err(convert_error)?;
                    nodes.iter_mut().for_each(Node::push_null);
                    return Ok(());
                }
                id = schema.resolve(chosen);
            }
            SchemaKind::Record { fields, .. } => {
                for (index, field) in fields.iter().enumerate() {
                    match projection[index] {
                        Some(output) => nodes[output].decode(dec, schema)?,
                        None => dec.skip(schema, field.ty).map_err(convert_error)?,
                    }
                }
                return Ok(());
            }
            _ => return Err(Error::internal("the root of an Avro file read as a record is not a record")),
        }
    }
}

struct ReadSingleAvroFile;

impl TableBind for ReadSingleAvroFile {
    fn bind(bind: &BindInfo, info: &TableBindInfo, context: &Context) -> Result<()> {
        // the file is a path, or a file struct that also holds the options to open it with - e.g. its size, as
        // globbing a remote store reported it
        let file_system = context.file_system()?;
        let (path, options) = file::file_argument(&file_system, &bind.arg_value(0)?)?;
        let data = file::read_file(context, &path, &options)?;
        let file = AvroFile::open(context, data)?;
        for column in &file.columns {
            info.add_result_column(&column.name, &column.type_)?;
        }
        bind.set_unique_bind_data(file)
    }
}

fn bind_data(data: Result<Option<&AvroFile>>) -> Result<&AvroFile> {
    data?.ok_or_else(|| Error::internal("read_single_avro_file: no bind data"))
}

impl TableInitGlobal for ReadSingleAvroFile {
    fn init_global(info: &TableInitGlobalInfo, _context: &Context) -> Result<()> {
        let file = bind_data(info.bind_data::<AvroFile>())?;
        info.set_global_state(GlobalState { next_block: AtomicUsize::new(0) })?;
        info.set_max_threads(file.blocks.len().max(1))
    }
}

impl TableInitLocal for ReadSingleAvroFile {
    fn init_local(info: &TableInitLocalInfo, _context: &Context) -> Result<()> {
        let file = bind_data(info.bind_data::<AvroFile>())?;
        let columns = info.column_indexes()?;
        let mut nodes = Vec::with_capacity(columns.len());
        let mut projection = vec![None; if file.root_is_struct { file.columns.len() } else { 0 }];
        for (output, &column) in columns.iter().enumerate() {
            let Some(definition) = file.columns.get(column) else {
                return Err(Error::internal(format!("read_single_avro_file: column index {column} out of range")));
            };
            nodes.push(Node::new(definition));
            if file.root_is_struct {
                if projection[column].is_some() {
                    return Err(Error::internal(format!("read_single_avro_file: column {column} projected twice")));
                }
                projection[column] = Some(output);
            } else if output > 0 {
                return Err(Error::internal(
                    "read_single_avro_file: a single-column file projected to several columns",
                ));
            }
        }
        info.set_local_state(LocalState { claimed: None, block: None, nodes, projection })
    }
}

impl TableClaimBatch for ReadSingleAvroFile {
    fn claim_batch(info: &TableClaimBatchInfo<'_>, _context: &Context) -> Result<()> {
        let file = bind_data(info.bind_data::<AvroFile>())?;
        let global = info.global_state::<GlobalState>()?.ok_or_else(|| Error::internal("no global state"))?;
        let local = info.local_state::<LocalState>()?.ok_or_else(|| Error::internal("no local state"))?;
        let block = global.next_block.fetch_add(1, Ordering::Relaxed);
        if block >= file.blocks.len() {
            return info.set_claimed(false);
        }
        local.claimed = Some(block);
        local.block = None;
        info.set_claimed(true)
    }
}

impl TableExec for ReadSingleAvroFile {
    fn exec(info: &TableExecInfo<'_>, _context: &Context) -> Result<()> {
        let file = bind_data(info.bind_data::<AvroFile>())?;
        let local = info.local_state::<LocalState>()?.ok_or_else(|| Error::internal("no local state"))?;
        let output = info.output()?;
        if let Some(index) = local.claimed.take() {
            let block = &file.blocks[index];
            local.block = Some(CurrentBlock { data: file.decompress(block)?, position: 0, rows_left: block.row_count });
        }
        let LocalState { block, nodes, projection, .. } = local;
        let Some(current) = block.as_mut() else { return output.set_size(0) };
        let count = usize::try_from(current.rows_left).unwrap_or(usize::MAX).min(output.capacity()?);
        if count == 0 {
            // the block is exhausted: an empty chunk makes the engine claim the next one
            *block = None;
            return output.set_size(0);
        }
        nodes.iter_mut().for_each(Node::clear);
        let mut dec = Decoder::new(&current.data[current.position..]);
        for _ in 0..count {
            decode_row(&mut dec, file, nodes, projection)?;
        }
        current.position += dec.position();
        current.rows_left -= count as u64;
        for (index, node) in nodes.iter().enumerate() {
            node.flush(&output.vector(index)?)?;
        }
        output.set_size(count)
    }
}

impl TableGetBindInfo for ReadSingleAvroFile {
    /// Describes the file: the field ids of its columns, and its key-value metadata as options - which a multi-file
    /// reader exposes as the metadata of the reader of the file.
    fn get_bind_info(info: &TableGetBindInfoInfo, context: &Context) -> Result<()> {
        let file = bind_data(info.bind_data::<AvroFile>())?;
        let factory = context.factory()?;
        for (index, column) in file.columns.iter().enumerate() {
            let mut path = Vec::new();
            set_column_identifiers(info, factory, column, index, &mut path)?;
        }
        for (key, value) in &file.header.metadata {
            info.set_option(key, &Value::varchar(factory, &String::from_utf8_lossy(value))?)?;
        }
        Ok(())
    }
}

/// Reports the field id of `column` (at `path` below result column `index`) and of every field nested in it.
fn set_column_identifiers(
    info: &TableGetBindInfoInfo,
    factory: duckdb_ext_api::Factory,
    column: &Column,
    index: usize,
    path: &mut Vec<usize>,
) -> Result<()> {
    if let Some(field_id) = column.field_id {
        info.set_column_identifier(index, path, &Value::integer(factory, field_id)?)?;
    }
    for (child_index, child) in column.id_children().into_iter().enumerate() {
        path.push(child_index);
        set_column_identifiers(info, factory, child, index, path)?;
        path.pop();
    }
    Ok(())
}

/// Registers `read_single_avro_file` and `read_avro`.
pub(crate) fn register(extension: &Extension, context: &Context) -> Result<()> {
    let factory = context.factory()?;
    let function = TableFunction::new(factory)?;
    function.set_name(SINGLE_FILE_FUNCTION)?;
    function.signature()?.add_parameter("file", &LogicalType::new(factory, type_id::ANY)?)?;
    function
        .set_bind::<ReadSingleAvroFile>()?
        .set_init_global::<ReadSingleAvroFile>()?
        .set_init_local::<ReadSingleAvroFile>()?
        .set_claim_batch::<ReadSingleAvroFile>()?
        .set_get_bind_info::<ReadSingleAvroFile>()?
        .set_exec::<ReadSingleAvroFile>()?
        .set_projection_pushdown(true)?;
    extension.register_table_function(&function)?;

    let multi_file = MultiFileFunction::new(factory)?;
    multi_file
        .set_name(MULTI_FILE_FUNCTION)?
        .set_single_file_function(SINGLE_FILE_FUNCTION)?
        .set_reader_type(READER_TYPE)?;
    extension.register_multi_file_function(&multi_file)
}
