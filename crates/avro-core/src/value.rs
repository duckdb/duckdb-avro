//! A materialized Avro value, for tests and tools. The pull [`Decoder`] is the primary API; this tree is not meant
//! for the hot path.

use crate::decode::Decoder;
use crate::encode::Encoder;
use crate::error::{Error, Result};
use crate::json::Json;
use crate::schema::{Schema, SchemaId, SchemaKind};

/// Recursion depth after which materialization gives up.
const MAX_DEPTH: usize = 1000;

/// A decoded value; records and maps keep their entries in file order.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Boolean(bool),
    Int(i32),
    Long(i64),
    Float(f32),
    Double(f64),
    Bytes(Vec<u8>),
    String(String),
    /// Field values in schema order.
    Record(Vec<Value>),
    /// The symbol index.
    Enum(usize),
    Array(Vec<Value>),
    Map(Vec<(String, Value)>),
    /// The branch index and the branch's value.
    Union(usize, Box<Value>),
    Fixed(Vec<u8>),
}

impl Value {
    /// Reads one value of schema node `id`.
    pub fn read(dec: &mut Decoder<'_>, schema: &Schema, id: SchemaId) -> Result<Value> {
        read_depth(dec, schema, id, 0)
    }

    /// Encodes the value, which must match schema node `id`.
    pub fn write(&self, enc: &mut Encoder, schema: &Schema, id: SchemaId) -> Result<()> {
        write_depth(self, enc, schema, id, 0)
    }

    /// The JSON rendering the dump tool prints: unions as the branch value, bytes and fixed as lowercase hex, enums
    /// as their symbol, logical types as their raw value, floats widened to double, non-finite floats as the
    /// strings `"NaN"`, `"Infinity"`, `"-Infinity"`.
    pub fn to_json(&self, schema: &Schema, id: SchemaId) -> Json {
        let node = schema.resolved(id);
        match (self, &node.kind) {
            (Value::Null, _) => Json::Null,
            (Value::Boolean(b), _) => Json::Bool(*b),
            (Value::Int(i), _) => Json::Int(i64::from(*i)),
            (Value::Long(l), _) => Json::Int(*l),
            (Value::Float(f), _) => float_json(f64::from(*f)),
            (Value::Double(d), _) => float_json(*d),
            (Value::Bytes(b), _) | (Value::Fixed(b), _) => Json::String(hex(b)),
            (Value::String(s), _) => Json::String(s.clone()),
            (Value::Enum(i), SchemaKind::Enum { symbols, .. }) => {
                Json::String(symbols.get(*i).cloned().unwrap_or_else(|| i.to_string()))
            }
            (Value::Enum(i), _) => Json::Int(*i as i64),
            (Value::Record(values), SchemaKind::Record { fields, .. }) => Json::Object(
                fields.iter().zip(values).map(|(f, v)| (f.name.clone(), v.to_json(schema, f.ty))).collect(),
            ),
            (Value::Record(values), _) => Json::Array(values.iter().map(|v| v.to_json(schema, id)).collect()),
            (Value::Array(items), SchemaKind::Array { items: item_id, .. }) => {
                Json::Array(items.iter().map(|v| v.to_json(schema, *item_id)).collect())
            }
            (Value::Array(items), _) => Json::Array(items.iter().map(|v| v.to_json(schema, id)).collect()),
            (Value::Map(entries), SchemaKind::Map { values, .. }) => {
                Json::Object(entries.iter().map(|(k, v)| (k.clone(), v.to_json(schema, *values))).collect())
            }
            (Value::Map(entries), _) => {
                Json::Object(entries.iter().map(|(k, v)| (k.clone(), v.to_json(schema, id))).collect())
            }
            (Value::Union(branch, value), SchemaKind::Union { branches }) => match branches.get(*branch) {
                Some(b) => value.to_json(schema, *b),
                None => value.to_json(schema, id),
            },
            (Value::Union(_, value), _) => value.to_json(schema, id),
        }
    }
}

fn float_json(f: f64) -> Json {
    if f.is_nan() {
        Json::String("NaN".to_owned())
    } else if f.is_infinite() {
        Json::String(if f > 0.0 { "Infinity" } else { "-Infinity" }.to_owned())
    } else {
        Json::Float(f)
    }
}

/// Lowercase hex of `bytes`.
pub fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(DIGITS[(b >> 4) as usize] as char);
        out.push(DIGITS[(b & 0xf) as usize] as char);
    }
    out
}

fn read_depth(dec: &mut Decoder<'_>, schema: &Schema, id: SchemaId, depth: usize) -> Result<Value> {
    if depth > MAX_DEPTH {
        return Err(Error::invalid_input("Avro value nesting too deep"));
    }
    Ok(match &schema[id].kind {
        SchemaKind::Null => Value::Null,
        SchemaKind::Boolean => Value::Boolean(dec.read_boolean()?),
        SchemaKind::Int => Value::Int(dec.read_int()?),
        SchemaKind::Long => Value::Long(dec.read_long()?),
        SchemaKind::Float => Value::Float(dec.read_float()?),
        SchemaKind::Double => Value::Double(dec.read_double()?),
        SchemaKind::Bytes => Value::Bytes(dec.read_bytes()?.to_vec()),
        SchemaKind::String => Value::String(dec.read_string()?.to_owned()),
        SchemaKind::Fixed { size, .. } => Value::Fixed(dec.read_fixed(*size)?.to_vec()),
        SchemaKind::Enum { symbols, .. } => Value::Enum(dec.read_enum(symbols.len())?),
        SchemaKind::Record { fields, .. } => {
            let mut values = Vec::with_capacity(fields.len());
            for field in fields {
                values.push(read_depth(dec, schema, field.ty, depth + 1)?);
            }
            Value::Record(values)
        }
        SchemaKind::Union { branches } => {
            let branch = dec.read_union_index(branches.len())?;
            Value::Union(branch, Box::new(read_depth(dec, schema, branches[branch], depth + 1)?))
        }
        SchemaKind::Array { items, .. } => {
            let mut out = Vec::new();
            let min_item_size = schema.min_encoded_size(*items);
            loop {
                let block = dec.read_block_count(min_item_size)?;
                if block.count == 0 {
                    break;
                }
                out.reserve(block.count.min(dec.remaining()));
                for _ in 0..block.count {
                    out.push(read_depth(dec, schema, *items, depth + 1)?);
                }
            }
            Value::Array(out)
        }
        SchemaKind::Map { values, .. } => {
            let mut out = Vec::new();
            // the key is a string: a length prefix at least
            let min_item_size = schema.min_encoded_size(*values).saturating_add(1);
            loop {
                let block = dec.read_block_count(min_item_size)?;
                if block.count == 0 {
                    break;
                }
                out.reserve(block.count.min(dec.remaining()));
                for _ in 0..block.count {
                    let key = dec.read_string()?.to_owned();
                    out.push((key, read_depth(dec, schema, *values, depth + 1)?));
                }
            }
            Value::Map(out)
        }
        SchemaKind::Ref { target, .. } => read_depth(dec, schema, *target, depth + 1)?,
    })
}

fn mismatch(value: &Value, kind: &SchemaKind) -> Error {
    Error::invalid_input(format!("Avro value {value:?} does not match schema type {}", kind.type_name()))
}

fn write_depth(value: &Value, enc: &mut Encoder, schema: &Schema, id: SchemaId, depth: usize) -> Result<()> {
    if depth > MAX_DEPTH {
        return Err(Error::invalid_input("Avro value nesting too deep"));
    }
    let kind = &schema[id].kind;
    match (value, kind) {
        (Value::Null, SchemaKind::Null) => {}
        (Value::Boolean(b), SchemaKind::Boolean) => enc.write_boolean(*b),
        (Value::Int(i), SchemaKind::Int) => enc.write_int(*i),
        (Value::Long(l), SchemaKind::Long) => enc.write_long(*l),
        (Value::Float(f), SchemaKind::Float) => enc.write_float(*f),
        (Value::Double(d), SchemaKind::Double) => enc.write_double(*d),
        (Value::Bytes(b), SchemaKind::Bytes) => enc.write_bytes(b),
        (Value::String(s), SchemaKind::String) => enc.write_string(s),
        (Value::Fixed(b), SchemaKind::Fixed { size, .. }) => {
            if b.len() != *size {
                return Err(Error::invalid_input(format!(
                    "fixed value of {} bytes for a fixed of size {size}",
                    b.len()
                )));
            }
            enc.write_fixed(b);
        }
        (Value::Enum(i), SchemaKind::Enum { symbols, .. }) => {
            if *i >= symbols.len() {
                return Err(Error::invalid_input(format!("Enum value out of range: {i}")));
            }
            enc.write_enum(*i);
        }
        (Value::Record(values), SchemaKind::Record { fields, .. }) => {
            if values.len() != fields.len() {
                return Err(Error::invalid_input("Avro record has an unexpected number of fields"));
            }
            for (field, v) in fields.iter().zip(values) {
                write_depth(v, enc, schema, field.ty, depth + 1)?;
            }
        }
        (Value::Union(branch, v), SchemaKind::Union { branches }) => {
            let target =
                *branches.get(*branch).ok_or_else(|| Error::invalid_input(format!("Invalid union tag {branch}")))?;
            enc.write_union_index(*branch);
            write_depth(v, enc, schema, target, depth + 1)?;
        }
        (Value::Array(items), SchemaKind::Array { items: item_id, .. }) => {
            if !items.is_empty() {
                enc.write_block_count(items.len());
                for item in items {
                    write_depth(item, enc, schema, *item_id, depth + 1)?;
                }
            }
            enc.write_block_end();
        }
        (Value::Map(entries), SchemaKind::Map { values, .. }) => {
            if !entries.is_empty() {
                enc.write_block_count(entries.len());
                for (k, v) in entries {
                    enc.write_string(k);
                    write_depth(v, enc, schema, *values, depth + 1)?;
                }
            }
            enc.write_block_end();
        }
        (_, SchemaKind::Ref { target, .. }) => write_depth(value, enc, schema, *target, depth + 1)?,
        _ => return Err(mismatch(value, kind)),
    }
    Ok(())
}
