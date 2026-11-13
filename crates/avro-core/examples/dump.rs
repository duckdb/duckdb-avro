//! Dumps an Avro object container file: metadata, codec, schema JSON, block index, and every row as one JSON text
//! per line (unions as the branch value, bytes/fixed as hex, logical types as their raw value).
//!
//! `dump FILE [--header] [--walk] [--skip]`: `--header` prints only the header information, `--walk` pull-decodes
//! every value without materializing (reports the time and a checksum), `--skip` skips every row with
//! `Decoder::skip` (reports the time).

use std::io::Write;
use std::time::Instant;

use avro_core::{Decoder, FileReader, Json, Schema, SchemaId, SchemaKind, Value};

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let Some(path) = args.iter().find(|a| !a.starts_with("--")) else {
        eprintln!("usage: dump FILE [--header] [--walk] [--skip]");
        std::process::exit(2);
    };
    let flag = |name: &str| args.iter().any(|a| a == name);
    if let Err(e) = run(path, flag("--header"), flag("--walk"), flag("--skip")) {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}

fn run(path: &str, header_only: bool, walk: bool, skip: bool) -> Result<(), Box<dyn std::error::Error>> {
    let bytes = std::fs::read(path)?;
    let reader = FileReader::open(&bytes)?;
    let header = reader.header();
    let stdout = std::io::stdout();
    let mut out = std::io::BufWriter::new(stdout.lock());
    let metadata = Json::Object(
        header
            .metadata
            .iter()
            .map(|(k, v)| {
                let text = match std::str::from_utf8(v) {
                    Ok(s) => s.to_owned(),
                    Err(_) => format!("hex:{}", avro_core::value::hex(v)),
                };
                (k.clone(), Json::String(text))
            })
            .collect(),
    );
    writeln!(out, "codec: {}", header.codec.name())?;
    writeln!(out, "metadata: {}", metadata.to_string())?;
    writeln!(out, "schema: {}", header.schema.to_json())?;
    let counts: Vec<String> = reader.blocks().iter().map(|b| b.row_count.to_string()).collect();
    writeln!(out, "blocks: {} rows: {} per_block: [{}]", reader.blocks().len(), reader.total_rows(), counts.join(","))?;
    if header_only {
        return Ok(());
    }
    let schema = reader.schema();
    let root = schema.root();
    if walk || skip {
        let start = Instant::now();
        let mut checksum = Checksum::default();
        let mut rows = 0u64;
        for block in reader.blocks() {
            let data = reader.decompress(block)?;
            let mut dec = Decoder::new(&data);
            for _ in 0..block.row_count {
                if skip {
                    dec.skip(schema, root)?;
                } else {
                    walk_value(&mut dec, schema, root, &mut checksum)?;
                }
                rows += 1;
            }
            if !dec.is_empty() {
                return Err(format!("block {} has {} trailing bytes", block.index, dec.remaining()).into());
            }
        }
        let elapsed = start.elapsed();
        writeln!(
            out,
            "{}: {rows} rows in {:.3} s ({:.1} Mrows/s), checksum {:016x}",
            if skip { "skip" } else { "walk" },
            elapsed.as_secs_f64(),
            rows as f64 / elapsed.as_secs_f64() / 1e6,
            checksum.state
        )?;
        return Ok(());
    }
    writeln!(out, "--- rows")?;
    let mut text = String::new();
    for block in reader.blocks() {
        let data = reader.decompress(block)?;
        let mut dec = Decoder::new(&data);
        for _ in 0..block.row_count {
            let value = Value::read(&mut dec, schema, root)?;
            text.clear();
            value.to_json(schema, root).write_to(&mut text);
            writeln!(out, "{text}")?;
        }
        if !dec.is_empty() {
            return Err(format!("block {} has {} trailing bytes", block.index, dec.remaining()).into());
        }
    }
    Ok(())
}

/// FNV-1a over every decoded value, so the walk cannot be optimized away and corruption shows up.
#[derive(Default)]
struct Checksum {
    state: u64,
}

impl Checksum {
    fn new() -> Self {
        Checksum { state: 0xcbf2_9ce4_8422_2325 }
    }

    #[inline]
    fn bytes(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.state = (self.state ^ u64::from(b)).wrapping_mul(0x0100_0000_01b3);
        }
    }

    #[inline]
    fn word(&mut self, w: u64) {
        self.bytes(&w.to_le_bytes());
    }
}

/// The schema-driven pull walk the DuckDB layer will do, minus the vector writes.
fn walk_value(dec: &mut Decoder<'_>, schema: &Schema, id: SchemaId, sum: &mut Checksum) -> avro_core::Result<()> {
    if sum.state == 0 {
        *sum = Checksum::new();
    }
    match &schema[id].kind {
        SchemaKind::Null => {}
        SchemaKind::Boolean => sum.word(u64::from(dec.read_boolean()?)),
        SchemaKind::Int => sum.word(dec.read_int()? as u64),
        SchemaKind::Long => sum.word(dec.read_long()? as u64),
        SchemaKind::Float => sum.word(u64::from(dec.read_float()?.to_bits())),
        SchemaKind::Double => sum.word(dec.read_double()?.to_bits()),
        SchemaKind::Bytes => sum.bytes(dec.read_bytes()?),
        SchemaKind::String => sum.bytes(dec.read_string()?.as_bytes()),
        SchemaKind::Fixed { size, .. } => sum.bytes(dec.read_fixed(*size)?),
        SchemaKind::Enum { symbols, .. } => sum.word(dec.read_enum(symbols.len())? as u64),
        SchemaKind::Record { fields, .. } => {
            for field in fields {
                walk_value(dec, schema, field.ty, sum)?;
            }
        }
        SchemaKind::Union { branches } => {
            let branch = dec.read_union_index(branches.len())?;
            sum.word(branch as u64);
            walk_value(dec, schema, branches[branch], sum)?;
        }
        SchemaKind::Array { items, .. } => loop {
            let block = dec.read_block_count(schema.min_encoded_size(*items))?;
            if block.count == 0 {
                break;
            }
            for _ in 0..block.count {
                walk_value(dec, schema, *items, sum)?;
            }
        },
        SchemaKind::Map { values, .. } => loop {
            let block = dec.read_block_count(schema.min_encoded_size(*values).saturating_add(1))?;
            if block.count == 0 {
                break;
            }
            for _ in 0..block.count {
                sum.bytes(dec.read_string()?.as_bytes());
                walk_value(dec, schema, *values, sum)?;
            }
        },
        SchemaKind::Ref { target, .. } => walk_value(dec, schema, *target, sum)?,
    }
    Ok(())
}
