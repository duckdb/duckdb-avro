//! `COPY ... TO 'file' (FORMAT avro)`: the write side of the extension, a port of the C++ extension's `avro_copy.cpp`
//! on the same C API v2 copy-function callbacks.
//!
//! - bind: reads the columns and options, builds the Avro schema ([`schema`]), the header metadata and the codec;
//! - init: opens the file and writes the container header (one file per init; `PARTITION_BY` makes many);
//! - batch: takes the rows of a batch (encoded later: the blocks of a file are written one at a time);
//! - flush: encodes every chunk of the batch as one block, in batch order ([`encode`]);
//! - finalize: closes the file;
//! - statistics: the exact bytes written and the row count for `RETURN_STATS`.
//!
//! Options (names case-insensitive; an option without a value reads as BOOLEAN `true`): `ROOT_NAME` (VARCHAR, the name
//! of the top-level record, `root` by default), `FIELD_IDS` (STRUCT, see [`field_ids`]), `SANITIZE_FIELD_NAMES`
//! (BOOLEAN), `METADATA` (STRUCT of string header entries), `CODEC` (VARCHAR: `null`, `deflate`, `snappy`, `zstandard`,
//! `bzip2`, `xz`, `lzma`). Unknown options are an `Invalid Configuration` error.

mod encode;
mod field_ids;
mod schema;
mod types;

/// The pure value conversions the proof harnesses of `crate::verification` check.
#[cfg(kani)]
pub(crate) use encode::{hugeint_to_i128, uuid_bytes, write_decimal};
#[cfg(kani)]
pub(crate) use schema::min_bytes_required_for_decimal;

use std::io::Write;
use std::sync::Mutex;

use avro_core::{Codec, Encoder, ErrorKind, FileWriter};
use duckdb_ext_api::sync::lock_ignore_poison;
use duckdb_ext_api::sys::{DUCKDB_V2_FILE_FLAG_CREATE_NEW, DUCKDB_V2_FILE_FLAG_WRITE};
use duckdb_ext_api::types::type_id;
use duckdb_ext_api::{
    ColumnDataCollection, Context, CopyFunction, CopyToBatch, CopyToBatchInfo, CopyToBind, CopyToBindInfo,
    CopyToFinalize, CopyToFinalizeInfo, CopyToFlush, CopyToFlushInfo, CopyToInit, CopyToInitInfo, CopyToStatistics,
    CopyToStatisticsInfo, DataChunk, Error, Extension, File, LogicalType, Result, Value,
};

use encode::ColumnPlan;
use schema::SchemaGenerator;

/// Registers the `avro` copy function (write-only).
pub(crate) fn register(extension: &Extension, context: &Context) -> Result<()> {
    let function = CopyFunction::new(context.factory()?)?;
    function.set_name("avro")?.set_file_extension("avro")?;
    function
        .set_to_bind::<AvroCopy>()?
        .set_to_init::<AvroCopy>()?
        .set_to_batch::<AvroCopy>()?
        .set_to_flush::<AvroCopy>()?
        .set_to_finalize::<AvroCopy>()?
        .set_to_statistics::<AvroCopy>()?;
    extension.register_copy_function(&function)
}

//===--------------------------------------------------------------------===//
// Options
//===--------------------------------------------------------------------===//

/// The options of a COPY statement, each marked as recognized when looked up.
struct CopyOptions {
    options: Vec<(String, Value)>,
    recognized: Vec<bool>,
}

impl CopyOptions {
    fn new(options: Vec<(String, Value)>) -> CopyOptions {
        let recognized = vec![false; options.len()];
        CopyOptions { options, recognized }
    }

    /// An option given without a value: the engine passes BOOLEAN `true`.
    fn is_bare(value: &Value) -> Result<bool> {
        Ok(!value.is_null()? && value.logical_type()?.id()? == type_id::BOOLEAN && value.as_bool()?)
    }

    /// The value of an option, marked as recognized; `None` when the statement does not set it.
    fn find(&mut self, name: &str) -> Option<&Value> {
        let index = self.options.iter().position(|(option, _)| option.eq_ignore_ascii_case(name))?;
        self.recognized[index] = true;
        Some(&self.options[index].1)
    }

    /// The value of an option that requires one.
    fn find_with_value(&mut self, name: &str) -> Result<Option<&Value>> {
        match self.find(name) {
            Some(value) if CopyOptions::is_bare(value)? => {
                Err(Error::invalid_input(format!("{name} can not be provided without a value")))
            }
            other => Ok(other),
        }
    }

    fn verify_all_recognized(&self) -> Result<()> {
        let mut unrecognized = String::new();
        for ((name, value), recognized) in self.options.iter().zip(&self.recognized) {
            if *recognized {
                continue;
            }
            if !unrecognized.is_empty() {
                unrecognized.push_str(", ");
            }
            unrecognized.push_str(&format!("key: \"{name}\""));
            if !CopyOptions::is_bare(value)? {
                unrecognized.push_str(&format!(" with value: '{}'", value.to_text()?));
            }
        }
        if !unrecognized.is_empty() {
            return Err(Error::invalid_configuration(format!(
                "The following option(s) are not recognized: {unrecognized}"
            )));
        }
        Ok(())
    }
}

/// The `METADATA` option: every child of the STRUCT becomes a string header entry.
fn parse_metadata(options: &mut CopyOptions) -> Result<Vec<(String, Vec<u8>)>> {
    let Some(value) = options.find_with_value("METADATA")? else {
        return Ok(Vec::new());
    };
    let type_ = value.logical_type()?;
    if type_.id()? != type_id::STRUCT {
        return Err(Error::invalid_input(
            "'METADATA' is expected to be provided as a STRUCT of key-value string metadata",
        ));
    }
    let mut metadata = Vec::new();
    for (i, (key, _)) in types::struct_children(&type_)?.into_iter().enumerate() {
        metadata.push((key, value.child(i)?.to_text()?.into_bytes()));
    }
    Ok(metadata)
}

/// The `CODEC` option: the lowercased codec name, `None` when unset (the writer defaults to `null`).
fn parse_codec(options: &mut CopyOptions) -> Result<Option<String>> {
    let Some(value) = options.find_with_value("CODEC")? else {
        return Ok(None);
    };
    if value.logical_type()?.id()? != type_id::VARCHAR {
        return Err(Error::invalid_input("'CODEC' is expected to be provided as VARCHAR (e.g. 'deflate', 'null')"));
    }
    Ok(Some(value.as_str()?.to_ascii_lowercase()))
}

fn parse_root_name(options: &mut CopyOptions, generator: &mut SchemaGenerator<'_>) -> Result<()> {
    let Some(value) = options.find_with_value("ROOT_NAME")? else {
        return Ok(());
    };
    if value.logical_type()?.id()? != type_id::VARCHAR {
        return Err(Error::invalid_input(
            "'ROOT_NAME' is expected to be provided as VARCHAR, this is used for the name of the top level 'record'",
        ));
    }
    generator.root_name = value.as_str()?;
    Ok(())
}

fn parse_sanitize_field_names(options: &mut CopyOptions, generator: &mut SchemaGenerator<'_>) -> Result<()> {
    let Some(value) = options.find("SANITIZE_FIELD_NAMES") else {
        return Ok(());
    };
    if value.is_null()? || value.logical_type()?.id()? != type_id::BOOLEAN {
        return Err(Error::invalid_input("SANITIZE_FIELD_NAMES requires a non-NULL BOOLEAN value"));
    }
    generator.sanitize_field_names = value.as_bool()?;
    Ok(())
}

/// An avro-core error as the engine's: the sink's I/O error keeps its text (`IO Error: ... No space left on device`).
fn avro_error(error: avro_core::Error) -> Error {
    match error.kind() {
        ErrorKind::Io => Error::io(error.message()),
        ErrorKind::NotImplemented => Error::not_implemented(error.message()),
        ErrorKind::InvalidInput => Error::invalid_input(error.message()),
    }
}

//===--------------------------------------------------------------------===//
// Bind
//===--------------------------------------------------------------------===//

/// What a bound COPY TO knows: the columns, the schema JSON, the header metadata and the codec.
struct BindData {
    types: Vec<LogicalType>,
    /// The column plans, mirroring the schema's nullability (built from the same FIELD_IDS).
    plans: Vec<ColumnPlan>,
    schema_json: String,
    metadata: Vec<(String, Vec<u8>)>,
    codec: Codec,
}

impl BindData {
    fn new(info: &CopyToBindInfo, context: &Context) -> Result<BindData> {
        let factory = context.factory()?;
        let (names, types): (Vec<String>, Vec<LogicalType>) = info.columns()?.into_iter().unzip();
        let mut options = CopyOptions::new(info.options()?);
        let metadata = parse_metadata(&mut options)?;

        let mut generator = SchemaGenerator::new(factory, &names, &types);
        if let Some(value) = options.find_with_value("FIELD_IDS")? {
            generator.field_ids = field_ids::parse_field_ids(factory, value, &names, &types)?;
        }
        parse_root_name(&mut options, &mut generator)?;
        parse_sanitize_field_names(&mut options, &mut generator)?;
        let schema = generator.generate()?;
        let mut plans = Vec::with_capacity(types.len());
        for (name, type_) in names.iter().zip(&types) {
            plans.push(ColumnPlan::new(type_, generator.field_ids.find(name))?);
        }

        let codec_name = parse_codec(&mut options)?;
        options.verify_all_recognized()?;
        // the C++ leaves the codec name to avro-c, which rejects an unknown one when the file is created
        let codec = match codec_name {
            Some(name) => Codec::from_name(&name).map_err(|e| Error::invalid_input(e.message()))?,
            None => Codec::Null,
        };
        Ok(BindData { types, plans, schema_json: schema.to_json(), metadata, codec })
    }
}

//===--------------------------------------------------------------------===//
// Writing a file
//===--------------------------------------------------------------------===//

/// The engine's file as the `Write` sink of the container writer; a short write is an error, as in the C++.
struct FileSink {
    file: File,
}

impl Write for FileSink {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self.file.write(buf) {
            Ok(0) if !buf.is_empty() => Err(std::io::Error::other("Could not write to the Avro file")),
            Ok(written) => Ok(written),
            Err(error) => Err(std::io::Error::other(error.message)),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// The state of one file being written: the container writer over the open file, the column plans, the counts.
struct FileState {
    writer: FileWriter<FileSink>,
    types: Vec<LogicalType>,
    plans: Vec<ColumnPlan>,
    /// The block buffer, reused across chunks.
    encoder: Encoder,
    row_count: u64,
}

impl FileState {
    fn open(context: &Context, bind_data: &BindData, path: &str) -> Result<FileState> {
        let file_system = context.file_system()?;
        let options = file_system.open_options()?;
        options.set_flag(DUCKDB_V2_FILE_FLAG_WRITE)?.set_flag(DUCKDB_V2_FILE_FLAG_CREATE_NEW)?;
        let file = file_system.open(path, &options)?;
        // the header is written here; a failure drops (and closes) the file
        let writer = FileWriter::with_schema_json(
            FileSink { file },
            &bind_data.schema_json,
            bind_data.codec,
            &bind_data.metadata,
        )
        .map_err(avro_error)?;
        let mut types = Vec::with_capacity(bind_data.types.len());
        for type_ in &bind_data.types {
            types.push(type_.try_clone()?);
        }
        Ok(FileState { writer, types, plans: bind_data.plans.clone(), encoder: Encoder::new(), row_count: 0 })
    }

    /// Writes the rows of a chunk to the file as one block.
    fn write_chunk(&mut self, chunk: &DataChunk) -> Result<()> {
        let count = chunk.size()?;
        if count == 0 {
            return Ok(());
        }
        let mut prepared = Vec::with_capacity(self.plans.len());
        for (index, plan) in self.plans.iter().enumerate() {
            prepared.push(plan.prepare(&chunk.vector(index)?)?);
        }
        self.encoder.clear();
        for row in 0..count {
            for column in &prepared {
                column.write(&mut self.encoder, row)?;
            }
        }
        self.writer.write_block(count as u64, self.encoder.as_bytes()).map_err(avro_error)?;
        self.row_count += count as u64;
        Ok(())
    }

    fn close(&self) -> Result<()> {
        self.writer.get_ref().file.close()
    }
}

/// The init data: the file state behind a mutex (flushes of one file never run concurrently, the statistics and
/// finalize callbacks come after them; the lock keeps the shared slot sound regardless).
struct InitData {
    state: Mutex<FileState>,
}

/// The batch data: the rows of a batch, encoded when the batch is flushed.
struct BatchData {
    collection: ColumnDataCollection,
}

//===--------------------------------------------------------------------===//
// Callbacks
//===--------------------------------------------------------------------===//

struct AvroCopy;

fn missing(what: &str) -> Error {
    Error::internal(format!("avro COPY TO: the {what} is missing"))
}

impl CopyToBind for AvroCopy {
    fn bind(info: &CopyToBindInfo, context: &Context) -> Result<()> {
        info.set_bind_data(BindData::new(info, context)?)
    }
}

impl CopyToInit for AvroCopy {
    fn init(info: &CopyToInitInfo, context: &Context) -> Result<()> {
        let bind_data = info.bind_data::<BindData>()?.ok_or_else(|| missing("bind data"))?;
        let state = FileState::open(context, bind_data, &info.file_path()?)?;
        info.set_init_data(InitData { state: Mutex::new(state) })
    }
}

impl CopyToBatch for AvroCopy {
    fn batch(info: &CopyToBatchInfo, _context: &Context) -> Result<()> {
        // the rows are encoded when the batch is flushed, as the blocks of a file are written one at a time
        info.set_batch_data(BatchData { collection: info.take_input()? })
    }
}

impl CopyToFlush for AvroCopy {
    fn flush(info: &CopyToFlushInfo<'_>, context: &Context) -> Result<()> {
        let init_data = info.init_data::<InitData>()?.ok_or_else(|| missing("init data"))?;
        let batch = info.batch_data::<BatchData>()?.ok_or_else(|| missing("batch data"))?;
        let mut state = lock_ignore_poison(&init_data.state);
        let shared = batch.collection.shared_scan_state()?;
        let mut worker = batch.collection.worker_scan_state()?;
        let types: Vec<&LogicalType> = state.types.iter().collect();
        let chunk = DataChunk::new(context.factory()?, &types)?;
        while batch.collection.scan(&shared, &mut worker, &chunk)? {
            state.write_chunk(&chunk)?;
        }
        Ok(())
    }
}

impl CopyToFinalize for AvroCopy {
    fn finalize(info: &CopyToFinalizeInfo, _context: &Context) -> Result<()> {
        // every block is already in the file; closing commits it on stores that commit on close
        let init_data = info.init_data::<InitData>()?.ok_or_else(|| missing("init data"))?;
        lock_ignore_poison(&init_data.state).close()
    }
}

impl CopyToStatistics for AvroCopy {
    fn statistics(info: &CopyToStatisticsInfo, _context: &Context) -> Result<()> {
        // the exact, incrementally tracked file size; footer size and column statistics do not apply to Avro
        let init_data = info.init_data::<InitData>()?.ok_or_else(|| missing("init data"))?;
        let state = lock_ignore_poison(&init_data.state);
        info.set_file_size(state.writer.bytes_written() as usize)?;
        info.set_row_count(state.row_count as usize)
    }
}
