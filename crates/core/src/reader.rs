//! `read_single_avro_file`, which reads one Avro object container file, and `read_avro`, the multi-file function on
//! top of it that adds globbing, lists of files, `filename`, `union_by_name`, hive partitioning and the like.
//!
//! A file is read into memory and its blocks are indexed at bind time; the scan then hands out one block per batch, so
//! a single file is read by several threads in parallel.

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use duckdb_neo::{
    Result, ToValue,
    bind_arguments::BindArgument,
    connection::Context,
    data_chunk::DataChunkRef,
    logical_type::LogicalTypeID,
    multi_file::MultiFileFunctionBuilder,
    signature::{FunctionParameter, SignatureBuilder},
    table_function::{
        BindFunctionHandle, BindInfoApi, ExecColumnInfo, InitColumnData, TableClaimBatchCallbacks,
        TableFunctionBuilder, TableFunctionCallbacks, TableGetBindInfoCallbacks,
    },
    types::Any,
    vector::Unknown,
};

use crate::avro::binary::Decoder;
use crate::avro::codec::Codec;
use crate::avro::container::{BlockInfo, FileInfo};
use crate::decode::{Column, fill_vector, skip};
use crate::error::{binder, invalid_input};
use crate::file::{FileArgument, read_file};
use crate::types::{AvroColumn, AvroType, transform_avro_type, transform_schema};

const SINGLE_FILE_FUNCTION: &str = "read_single_avro_file";

/// An Avro object container file, read into memory.
#[derive(Debug)]
pub struct AvroFile {
    buffer: Vec<u8>,
    info: FileInfo,
    avro_type: Arc<AvroType>,
    // A record root has its fields pulled up into the columns.
    root_is_struct: bool,
    columns: Vec<AvroColumn>,
}

impl AvroFile {
    fn open(context: &Context, file: &FileArgument) -> Result<AvroFile> {
        let buffer = read_file(context, file)?;
        let info = FileInfo::parse(&buffer)?;
        let schema = &info.schema;
        let root_name = match schema.node(schema.root()) {
            crate::avro::schema::Node::Record { name, .. }
            | crate::avro::schema::Node::Enum { name, .. }
            | crate::avro::schema::Node::Fixed { name, .. } => name.clone(),
            _ => "avro_schema".to_owned(),
        };
        let avro_type = transform_schema(schema, schema.root())?;
        let root = transform_avro_type(context, &root_name, &avro_type)?;
        let root_is_struct = root.logical_type.id()? == LogicalTypeID::Struct;
        let columns = if root_is_struct {
            root.children
        } else {
            vec![root]
        };
        Ok(AvroFile {
            buffer,
            info,
            avro_type: Arc::new(avro_type),
            root_is_struct,
            columns,
        })
    }

    fn block_count(&self) -> usize {
        self.info.blocks.len()
    }
}

#[derive(Debug)]
enum RowReader {
    Value(Column),
    // The record may be wrapped in unions with NULL; fields that are not projected are skipped.
    Record {
        root: Arc<AvroType>,
        fields: Vec<Option<usize>>,
        columns: Vec<Column>,
    },
}

fn root_record(root: &AvroType) -> &AvroType {
    let mut record = root;
    while record.kind == crate::types::AvroKind::Union {
        let branch = record
            .single_member_branch()
            .expect("a STRUCT root has one member");
        record = &record.children[branch].1;
    }
    record
}

impl RowReader {
    fn new(file: &AvroFile, column_ids: &[usize]) -> Result<RowReader> {
        if !file.root_is_struct {
            if column_ids.iter().any(|column_id| *column_id != 0) {
                return Err(invalid_input(format!(
                    "Invalid column indexes {column_ids:?}"
                )));
            }
            return Ok(RowReader::Value(Column::new(&file.avro_type)?));
        }
        let record = root_record(&file.avro_type);
        let mut fields = vec![None; record.children.len()];
        let mut columns = Vec::with_capacity(column_ids.len());
        for (output, column_id) in column_ids.iter().enumerate() {
            let slot = fields
                .get_mut(*column_id)
                .ok_or_else(|| invalid_input(format!("Invalid column index {column_id}")))?;
            *slot = Some(output);
            columns.push(Column::new(&record.children[*column_id].1)?);
        }
        Ok(RowReader::Record {
            root: file.avro_type.clone(),
            fields,
            columns,
        })
    }

    fn columns(&self) -> &[Column] {
        match self {
            RowReader::Value(column) => std::slice::from_ref(column),
            RowReader::Record { columns, .. } => columns,
        }
    }

    fn clear(&mut self) {
        match self {
            RowReader::Value(column) => column.clear(),
            RowReader::Record { columns, .. } => columns.iter_mut().for_each(Column::clear),
        }
    }

    fn read_row(&mut self, decoder: &mut Decoder<'_>) -> Result<()> {
        let (root, fields, columns) = match self {
            RowReader::Value(column) => return column.read(decoder),
            RowReader::Record {
                root,
                fields,
                columns,
            } => (root, fields, columns),
        };
        let mut record: &AvroType = root;
        while record.kind == crate::types::AvroKind::Union {
            let discriminant = decoder
                .read_long()
                .map_err(|e| e.prefixed("Cannot read union discriminant: "))?;
            let branch = usize::try_from(discriminant)
                .ok()
                .filter(|branch| *branch < record.children.len())
                .ok_or_else(|| {
                    invalid_input(format!(
                        "Invalid union discriminant value: ({discriminant})"
                    ))
                })?;
            record = &record.children[branch].1;
            if record.type_id == LogicalTypeID::SqlNull {
                columns.iter_mut().for_each(Column::push_null);
                return Ok(());
            }
        }
        for (field, (_, field_type)) in record.children.iter().enumerate() {
            match fields[field] {
                Some(output) => columns[output].read(decoder)?,
                None => skip(field_type, decoder)?,
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
enum BlockData {
    File { offset: usize, size: usize },
    Scratch { size: usize },
}

/// The scan of one thread: it reads the blocks it claims, one at a time.
#[derive(Debug)]
pub struct ScanState {
    rows: RowReader,
    block: Option<BlockInfo>,
    data: Option<BlockData>,
    scratch: Vec<u8>,
    position: usize,
    rows_read: u64,
}

impl ScanState {
    fn select_block(&mut self, file: &AvroFile, block: &BlockInfo) -> Result<BlockData> {
        if file.info.codec == Codec::Null {
            return Ok(BlockData::File {
                offset: block.offset,
                size: block.size,
            });
        }
        let size = file
            .info
            .decode_block(&file.buffer, block, &mut self.scratch)?
            .len();
        Ok(BlockData::Scratch { size })
    }

    fn scan(&mut self, file: &AvroFile, mut output: DataChunkRef<'_>) -> Result<()> {
        let Some(block) = self.block else {
            return Ok(());
        };
        let data = match self.data {
            Some(data) => data,
            None => {
                let data = self.select_block(file, &block)?;
                self.data = Some(data);
                self.position = 0;
                self.rows_read = 0;
                data
            }
        };
        let bytes = match data {
            BlockData::File { offset, size } => &file.buffer[offset..offset + size],
            BlockData::Scratch { size } => &self.scratch[..size],
        };
        let mut decoder = Decoder::new(&bytes[self.position..]);
        self.rows.clear();
        let capacity = output.capacity()?;
        let mut count = 0;
        while count < capacity {
            if self.rows_read == block.object_count {
                if !decoder.is_empty() {
                    return Err(invalid_input("Avro file block contains trailing data"));
                }
                break;
            }
            if let Err(error) = self.rows.read_row(&mut decoder) {
                // Running out of data at a value boundary is what avro-c reports as the block ending early
                if decoder.is_empty() && error.message.ends_with("bytes from memory buffer") {
                    return Err(invalid_input(
                        "Avro file block ended before its declared object count",
                    ));
                }
                return Err(error);
            }
            self.rows_read += 1;
            count += 1;
        }
        self.position += decoder.position();

        for (index, column) in self.rows.columns().iter().enumerate() {
            let mut vector = output.get_vector_at_mut::<Unknown>(index)?;
            vector.set_size(count)?;
            fill_vector(vector, column)?;
        }
        Ok(())
    }
}

/// The table function behind `read_single_avro_file`.
#[derive(Debug)]
pub struct AvroScan;

impl TableFunctionCallbacks for AvroScan {
    type BindData = AvroFile;
    type LocalState = ScanState;
    type GlobalState = AtomicUsize;

    fn bind(
        &self,
        context: &Context,
        arguments: Vec<BindArgument>,
        bind_handle: BindFunctionHandle<'_>,
    ) -> Result<AvroFile> {
        // The file is a path, or a file struct that also holds the options to open it with - e.g. its size, as
        // globbing a remote store reported it
        let file = arguments
            .first()
            .and_then(|argument| argument.value.as_ref())
            .ok_or_else(|| {
                binder(format!(
                    "{SINGLE_FILE_FUNCTION} requires a constant file argument"
                ))
            })?;
        let file = AvroFile::open(context, &FileArgument::parse(file)?)?;
        for column in &file.columns {
            bind_handle.add_result_column(&column.name, &column.logical_type)?;
        }
        Ok(file)
    }

    fn init_global_state(
        &self,
        bind_data: &AvroFile,
        _context: &Context,
        _column_data: InitColumnData<'_>,
    ) -> Result<(Option<AtomicUsize>, Option<usize>)> {
        Ok((
            Some(AtomicUsize::new(0)),
            Some(bind_data.block_count().max(1)),
        ))
    }

    fn init_local_state(
        &self,
        bind_data: &AvroFile,
        _global_state: Option<&AtomicUsize>,
        _context: &Context,
        column_data: InitColumnData<'_>,
    ) -> Result<Option<ScanState>> {
        let column_ids = (0..column_data.count()?)
            .map(|index| column_data.get_column_index(index))
            .collect::<Result<Vec<_>>>()?;
        Ok(Some(ScanState {
            rows: RowReader::new(bind_data, &column_ids)?,
            block: None,
            data: None,
            scratch: Vec::new(),
            position: 0,
            rows_read: 0,
        }))
    }

    fn exec(
        &self,
        bind_data: &AvroFile,
        _global_state: Option<&AtomicUsize>,
        local_state: Option<&mut ScanState>,
        _context: &Context,
        output: DataChunkRef<'_>,
        _column_info: ExecColumnInfo<'_>,
    ) -> Result<()> {
        match local_state {
            Some(state) => state.scan(bind_data, output),
            None => Ok(()),
        }
    }
}

impl TableClaimBatchCallbacks for AvroScan {
    fn claim_batch(
        &self,
        bind_data: &AvroFile,
        global_state: Option<&AtomicUsize>,
        local_state: Option<&mut ScanState>,
        _context: &Context,
    ) -> Result<bool> {
        let (Some(next_block), Some(state)) = (global_state, local_state) else {
            return Ok(false);
        };
        let index = next_block.fetch_add(1, Ordering::Relaxed);
        state.block = bind_data.info.blocks.get(index).copied();
        state.data = None;
        Ok(state.block.is_some())
    }
}

fn set_column_identifiers(
    api: &BindInfoApi<'_>,
    context: &Context,
    column: &AvroColumn,
    column_index: usize,
    child_path: &mut Vec<u64>,
) -> Result<()> {
    if let Some(field_id) = column.field_id {
        api.set_column_identifier(column_index, child_path, &field_id.value(context)?)?;
    }
    // A MAP addresses its keys and values directly, without the key/value STRUCT in between
    let children = if column.logical_type.id()? == LogicalTypeID::Map {
        &column.children[0].children
    } else {
        &column.children
    };
    for (index, child) in children.iter().enumerate() {
        child_path.push(index as u64);
        set_column_identifiers(api, context, child, column_index, child_path)?;
        child_path.pop();
    }
    Ok(())
}

impl TableGetBindInfoCallbacks for AvroScan {
    /// Describes the file: the field ids of its columns, and its key-value metadata as options - which a multi-file
    /// reader exposes as the metadata of the reader of the file.
    fn bind_info(
        &self,
        bind_data: &AvroFile,
        context: &Context,
        api: BindInfoApi<'_>,
    ) -> Result<()> {
        for (index, column) in bind_data.columns.iter().enumerate() {
            set_column_identifiers(&api, context, column, index, &mut Vec::new())?;
        }
        for (key, value) in &bind_data.info.metadata {
            let value = String::from_utf8_lossy(value);
            api.set_option(key, &value.as_ref().value(context)?)?;
        }
        Ok(())
    }
}

/// `read_single_avro_file(file)`, which reads a single file given as a path or as a file struct.
pub fn single_file_function() -> TableFunctionBuilder<AvroScan> {
    let signature = SignatureBuilder::new().param(FunctionParameter::new::<Any>("file"));
    TableFunctionBuilder::new(SINGLE_FILE_FUNCTION, signature, AvroScan)
        .with_projection_pushdown(true)
        .with_claim_batch()
        .with_get_bind_info()
}

/// `read_avro`, the multi-file function on top of `read_single_avro_file`.
pub fn multi_file_function() -> MultiFileFunctionBuilder {
    MultiFileFunctionBuilder::new("read_avro", SINGLE_FILE_FUNCTION).reader_type("Avro")
}
