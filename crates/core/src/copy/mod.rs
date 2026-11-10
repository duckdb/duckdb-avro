//! `COPY ... TO (FORMAT AVRO)`: writes object container files with the options `FIELD_IDS`, `ROOT_NAME`,
//! `SANITIZE_FIELD_NAMES`, `METADATA` and `CODEC`.

mod encoder;
pub(crate) mod schema;
mod writer;

use std::sync::{Mutex, MutexGuard};

use duckdb_neo::{
    Result,
    column_data_collection::ColumnDataCollection,
    connection::Context,
    copy_function::{
        CopyFunctionBuilder, CopyToBindInfo, CopyToFunctionCallbacks, CopyToStatistics,
        CopyToStatisticsCallbacks,
    },
    logical_type::LogicalTypeID,
    value::Value,
    vector::Unknown,
};

use self::encoder::{ColumnEncoder, ColumnReader, column_encoders, flatten_chunk};
use self::schema::SchemaGenerator;
use self::writer::AvroFileWriter;
use crate::avro::codec::Codec;
use crate::avro::schema::Schema;
use crate::error::{internal, invalid_configuration, invalid_input};
use crate::field_ids::parse_field_ids;

struct CopyOptions {
    options: Vec<(String, Value)>,
    recognized: Vec<bool>,
}

fn is_bare(value: &Value) -> Result<bool> {
    Ok(!value.is_null()?
        && value.logical_type()?.id()? == LogicalTypeID::Boolean
        && value.get::<bool>()? == Some(true))
}

impl CopyOptions {
    fn new(bind_info: &CopyToBindInfo) -> Result<Self> {
        let count = bind_info.option_count()?;
        let mut options = Vec::with_capacity(count);
        for index in 0..count {
            let (name, value) = bind_info.get_option(index)?;
            options.push((name.to_owned(), value));
        }
        Ok(Self {
            options,
            recognized: vec![false; count],
        })
    }

    fn find(&mut self, name: &str) -> Option<&Value> {
        let index = self
            .options
            .iter()
            .position(|(option, _)| option.eq_ignore_ascii_case(name))?;
        self.recognized[index] = true;
        Some(&self.options[index].1)
    }

    fn find_with_value(&mut self, name: &str) -> Result<Option<&Value>> {
        match self.find(name) {
            Some(value) if is_bare(value)? => Err(invalid_input(format!(
                "{name} can not be provided without a value"
            ))),
            value => Ok(value),
        }
    }

    fn verify_all_recognized(&self) -> Result<()> {
        let mut unrecognized = Vec::new();
        for ((name, value), recognized) in self.options.iter().zip(&self.recognized) {
            if *recognized {
                continue;
            }
            let mut option = format!("key: \"{name}\"");
            if !is_bare(value)? {
                option.push_str(&format!(" with value: '{}'", value.dbg_string()?));
            }
            unrecognized.push(option);
        }
        if unrecognized.is_empty() {
            return Ok(());
        }
        Err(invalid_configuration(format!(
            "The following option(s) are not recognized: {}",
            unrecognized.join(", ")
        )))
    }
}

fn varchar_option(value: &Value) -> Result<Option<String>> {
    if value.logical_type()?.id()? != LogicalTypeID::Varchar {
        return Ok(None);
    }
    value.get::<String>()
}

fn parse_metadata(options: &mut CopyOptions) -> Result<Vec<(String, String)>> {
    let Some(value) = options.find_with_value("METADATA")? else {
        return Ok(Vec::new());
    };
    let logical_type = value.logical_type()?;
    if logical_type.id()? != LogicalTypeID::Struct {
        return Err(invalid_input(
            "'METADATA' is expected to be provided as a STRUCT of key-value string metadata",
        ));
    }
    let mut metadata = Vec::new();
    for (index, (key, _)) in logical_type.get_params()?.into_iter().enumerate() {
        metadata.push((key, value.get_child(index)?.dbg_string()?));
    }
    Ok(metadata)
}

fn parse_codec(options: &mut CopyOptions) -> Result<String> {
    let Some(value) = options.find_with_value("CODEC")? else {
        return Ok(String::new());
    };
    let codec = varchar_option(value)?.ok_or_else(|| {
        invalid_input("'CODEC' is expected to be provided as VARCHAR (e.g. 'deflate', 'null')")
    })?;
    Ok(codec.to_ascii_lowercase())
}

/// What a bound `COPY ... TO (FORMAT AVRO)` writes.
#[derive(Debug)]
pub struct AvroCopyBindData {
    schema_json: String,
    metadata: Vec<(String, String)>,
    codec: Codec,
    columns: Vec<ColumnEncoder>,
}

fn bind(context: &Context, bind_info: &CopyToBindInfo) -> Result<AvroCopyBindData> {
    let mut names = Vec::new();
    let mut types = Vec::new();
    for index in 0..bind_info.column_count()? {
        let (name, logical_type) = bind_info.get_column(index)?;
        names.push(name.to_owned());
        types.push(logical_type);
    }

    let mut options = CopyOptions::new(bind_info)?;
    let metadata = parse_metadata(&mut options)?;

    let mut generator = SchemaGenerator::new(&names, &types);
    if let Some(value) = options.find_with_value("FIELD_IDS")? {
        generator.field_ids = parse_field_ids(context, value, &names, &types)?;
    }
    if let Some(value) = options.find_with_value("ROOT_NAME")? {
        generator.root_name = varchar_option(value)?.ok_or_else(|| {
            invalid_input(
                "'ROOT_NAME' is expected to be provided as VARCHAR, this is used for the name of the top level 'record'",
            )
        })?;
    }
    if let Some(value) = options.find("SANITIZE_FIELD_NAMES") {
        let sanitize = match value.logical_type()?.id()? {
            LogicalTypeID::Boolean => value.get::<bool>()?,
            _ => None,
        };
        generator.sanitize_field_names = sanitize.ok_or_else(|| {
            invalid_input("SANITIZE_FIELD_NAMES requires a non-NULL BOOLEAN value")
        })?;
    }
    let schema_json = generator.generate()?;

    let codec_name = parse_codec(&mut options)?;
    options.verify_all_recognized()?;

    // Parsing validates the generated schema (e.g. its identifiers) and renders it the way files store it.
    let schema = Schema::parse(schema_json.as_bytes())?;
    let codec = if codec_name.is_empty() {
        Codec::Null
    } else {
        Codec::from_name(&codec_name)?
    };
    Ok(AvroCopyBindData {
        schema_json: schema.to_json(),
        metadata,
        codec,
        columns: column_encoders(&schema, &types)?,
    })
}

/// The state of one output file.
///
/// The callbacks only get shared access to it, hence the mutex.
pub struct AvroCopyInitData {
    writer: Mutex<AvroFileWriter>,
}

impl AvroCopyInitData {
    fn lock(&self) -> Result<MutexGuard<'_, AvroFileWriter>> {
        self.writer
            .lock()
            .map_err(|_| internal("The Avro file writer failed while writing"))
    }
}

fn write_rows(
    context: &Context,
    columns: &[ColumnEncoder],
    writer: &mut AvroFileWriter,
    input: &ColumnDataCollection<'_>,
) -> Result<()> {
    let mut scan = input.to_scan(context)?;
    let mut row = Vec::new();
    while let Some(mut chunk) = scan.next_chunk()? {
        flatten_chunk(&mut chunk)?;
        let mut readers = Vec::with_capacity(columns.len());
        for (index, column) in columns.iter().enumerate() {
            readers.push(ColumnReader::new(
                column,
                chunk.get_vector_at::<Unknown>(index)?,
            )?);
        }
        let row_count = chunk.row_count()?;
        for index in 0..row_count {
            row.clear();
            let mut estimate = 0;
            for reader in &readers {
                estimate += reader.encode(index, &mut row)?;
            }
            writer.append_row(&row, estimate)?;
        }
        writer.finish_chunk(row_count)?;
    }
    Ok(())
}

/// The `avro` format of `COPY ... TO`.
#[derive(Debug)]
pub struct AvroCopy;

impl CopyToFunctionCallbacks for AvroCopy {
    type InitData = AvroCopyInitData;
    type BindData = AvroCopyBindData;
    type BatchData = ();

    fn bind(&self, context: &Context, bind_info: &CopyToBindInfo) -> Result<Self::BindData> {
        bind(context, bind_info)
    }

    fn init(
        &self,
        context: &Context,
        bind_data: &Self::BindData,
        file_path: &str,
    ) -> Result<Self::InitData> {
        let writer = AvroFileWriter::create(
            context,
            file_path,
            &bind_data.schema_json,
            bind_data.codec,
            &bind_data.metadata,
        )?;
        Ok(AvroCopyInitData {
            writer: Mutex::new(writer),
        })
    }

    fn batch(
        &self,
        _context: &Context,
        _bind_data: &Self::BindData,
        _init_data: &Self::InitData,
        _input: &ColumnDataCollection<'_>,
    ) -> Result<Self::BatchData> {
        // Rows are encoded when the batch is flushed, as the blocks of a file are written one at a time.
        Ok(())
    }

    fn flush(
        &self,
        context: &Context,
        bind_data: &Self::BindData,
        init_data: &Self::InitData,
        input: &ColumnDataCollection<'_>,
        _batch_data: &mut Self::BatchData,
    ) -> Result<()> {
        let mut writer = init_data.lock()?;
        write_rows(context, &bind_data.columns, &mut writer, input)
    }

    fn finalize(
        &self,
        _context: &Context,
        _bind_data: &Self::BindData,
        init_data: &Self::InitData,
    ) -> Result<()> {
        // Every block is already written; closing commits the file on stores that only publish it on close.
        init_data.lock()?.close()
    }
}

impl CopyToStatisticsCallbacks for AvroCopy {
    fn statistics(
        &self,
        _context: &Context,
        _bind_data: &Self::BindData,
        init_data: &Self::InitData,
    ) -> Result<CopyToStatistics> {
        let writer = init_data.lock()?;
        Ok(CopyToStatistics {
            row_count: Some(writer.row_count()),
            file_size: Some(writer.bytes_written()),
        })
    }
}

/// The `avro` copy function, to be registered on an extension or a connection.
pub fn copy_function() -> CopyFunctionBuilder<AvroCopy> {
    CopyFunctionBuilder::new("avro", AvroCopy).with_statistics()
}
