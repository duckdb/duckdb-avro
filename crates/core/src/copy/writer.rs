//! Writes the object container file of one `COPY ... TO (FORMAT AVRO)` output file.

use duckdb_neo::{
    Result,
    connection::Context,
    file::{File, FileBuilder, FileSystem},
};

use crate::avro::codec::Codec;
use crate::avro::container::{SYNC_SIZE, generate_sync, write_block, write_header};
use crate::error::invalid_input;

fn next_power_of_two(value: usize) -> usize {
    value.max(1).next_power_of_two()
}

fn write_all(file: &File, mut data: &[u8], bytes_written: &mut usize) -> Result<()> {
    while !data.is_empty() {
        let written = file.write(data)?.min(data.len());
        if written == 0 {
            return Err(invalid_input("Could not write to the Avro file"));
        }
        data = &data[written..];
        *bytes_written += written;
    }
    Ok(())
}

/// The file being written and its current block.
///
/// Blocks are split the way avro-c's memory-buffer writer splits them, which keeps file sizes stable: rows go to
/// a buffer whose capacity grows from size estimates of the rows (in powers of two), and a block is written
/// whenever a row does not fit in the rest of that buffer.
pub struct AvroFileWriter {
    file: File,
    codec: Codec,
    sync: [u8; SYNC_SIZE],
    datum_capacity: usize,
    block: Vec<u8>,
    block_rows: u64,
    output: Vec<u8>,
    scratch: Vec<u8>,
    bytes_written: usize,
    row_count: usize,
}

impl AvroFileWriter {
    /// Creates the file at `path` and writes its header.
    pub fn create(
        context: &Context,
        path: &str,
        schema_json: &str,
        codec: Codec,
        metadata: &[(String, String)],
    ) -> Result<Self> {
        let file_system = FileSystem::new(context)?;
        let file = FileBuilder::new(&file_system, path)?
            .write()
            .create_new()
            .open()?;
        let mut writer = Self {
            file,
            codec,
            sync: generate_sync(),
            datum_capacity: 0,
            block: Vec::new(),
            block_rows: 0,
            output: Vec::new(),
            scratch: Vec::new(),
            bytes_written: 0,
            row_count: 0,
        };
        write_header(
            &mut writer.output,
            schema_json,
            codec,
            metadata,
            &writer.sync,
        );
        writer.write_output()?;
        Ok(writer)
    }

    /// The number of bytes written to the file so far.
    pub fn bytes_written(&self) -> usize {
        self.bytes_written
    }

    /// The number of rows written to the file so far.
    pub fn row_count(&self) -> usize {
        self.row_count
    }

    /// Adds an encoded row to the current block, of which `estimate` is the size estimate.
    pub fn append_row(&mut self, encoded: &[u8], estimate: usize) -> Result<()> {
        if estimate > self.datum_capacity - self.block.len() {
            self.datum_capacity = next_power_of_two(self.datum_capacity + estimate);
        }
        loop {
            if self.block.len() + encoded.len() <= self.datum_capacity {
                break;
            }
            if self.block_rows > 0 {
                self.write_block()?;
            }
            if self.block.len() + encoded.len() <= self.datum_capacity {
                break;
            }
            self.datum_capacity = next_power_of_two(self.datum_capacity * 2);
        }
        self.block.extend_from_slice(encoded);
        self.block_rows += 1;
        Ok(())
    }

    /// Writes the current block at the end of a chunk of `rows` rows.
    pub fn finish_chunk(&mut self, rows: usize) -> Result<()> {
        if self.block_rows > 0 {
            self.write_block()?;
        }
        self.row_count += rows;
        Ok(())
    }

    /// Closes the file.
    pub fn close(&self) -> Result<()> {
        self.file.close()
    }

    fn write_block(&mut self) -> Result<()> {
        write_block(
            &mut self.output,
            self.block_rows,
            &self.block,
            self.codec,
            &self.sync,
            &mut self.scratch,
        )?;
        self.block.clear();
        self.block_rows = 0;
        self.write_output()
    }

    fn write_output(&mut self) -> Result<()> {
        let result = write_all(&self.file, &self.output, &mut self.bytes_written);
        self.output.clear();
        result
    }
}
