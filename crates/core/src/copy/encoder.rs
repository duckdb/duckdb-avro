//! Encodes the rows of DuckDB vectors as Avro binary data, following the schema of the file.

use duckdb_neo::{
    Result,
    data_chunk::{ChildrenMut, DataChunkRef},
    logical_type::{LogicalType, LogicalTypeID},
    types::{
        BlobValue, DateValue, DecimalSignature, HugeDecimal, HugeInt, List, ListIndex, LongDecimal,
        Map, ShortDecimal, Struct, TimeValue, TimestampMsValue, TimestampNsValue,
        TimestampTzNsValue, TimestampTzValue, TimestampValue, UncheckedString, Union, UuidValueRaw,
        WordDecimal,
    },
    vector::{Unknown, Vector, VectorElement, VectorReader},
};

use super::schema::{EMPTY_STRUCT_MARKER, decimal_fixed_size, struct_children, type_param};
use crate::avro::binary::{
    write_boolean, write_bytes, write_double, write_float, write_int, write_long,
};
use crate::avro::schema::{Node, NodeId, Schema};
use crate::error::{internal, invalid_input};

#[derive(Debug)]
enum EncoderKind {
    Null,
    Boolean,
    Integer,
    BigInt,
    Float,
    Double,
    Varchar,
    Blob,
    Date,
    Time,
    Timestamp,
    TimestampMs,
    TimestampNs,
    TimestampTz,
    TimestampTzNs,
    Uuid,
    Decimal {
        width: u8,
        size: usize,
    },
    Struct(Vec<(usize, ColumnEncoder)>),
    List(Box<ColumnEncoder>),
    Map {
        key: Box<ColumnEncoder>,
        value: Box<ColumnEncoder>,
    },
    Enum(EnumIndexWidth),
    Union(Vec<ColumnEncoder>),
}

#[derive(Debug, Clone, Copy)]
enum EnumIndexWidth {
    U8,
    U16,
    U32,
}

impl EnumIndexWidth {
    fn for_symbol_count(count: usize) -> Self {
        if count <= usize::from(u8::MAX) {
            Self::U8
        } else if count <= usize::from(u16::MAX) {
            Self::U16
        } else {
            Self::U32
        }
    }
}

/// How the values of one column (or nested field) are encoded: its Avro type is nullable when it is the union
/// `["null", T]`.
#[derive(Debug)]
pub struct ColumnEncoder {
    type_name: String,
    nullable: bool,
    kind: EncoderKind,
}

fn schema_mismatch(logical_type: &LogicalType) -> duckdb_neo::Error {
    internal(format!(
        "The Avro schema does not match the column type {}",
        logical_type.to_text().unwrap_or_default()
    ))
}

fn non_null_branch(schema: &Schema, node: NodeId) -> (bool, NodeId) {
    let node = schema.resolve(node);
    if let Node::Union { branches } = schema.node(node)
        && let [null, value] = branches.as_slice()
        && matches!(schema.node(schema.resolve(*null)), Node::Null)
    {
        return (true, schema.resolve(*value));
    }
    (false, node)
}

/// The encoders of the columns of the root record of `schema`.
pub fn column_encoders(schema: &Schema, types: &[LogicalType]) -> Result<Vec<ColumnEncoder>> {
    let Node::Record { fields, .. } = schema.node(schema.resolve(schema.root())) else {
        return Err(internal("The Avro schema of the file is not a record"));
    };
    if fields.len() != types.len() {
        return Err(internal("The Avro schema does not match the columns"));
    }
    types
        .iter()
        .zip(fields)
        .map(|(logical_type, field)| ColumnEncoder::new(logical_type, schema, field.schema))
        .collect()
}

fn union_branches(schema: &Schema, node: NodeId, member_count: usize) -> Option<(bool, &[NodeId])> {
    let Node::Union { branches } = schema.node(schema.resolve(node)) else {
        return None;
    };
    if branches.len() == member_count {
        return Some((false, branches));
    }
    match branches.split_first() {
        Some((null, members))
            if members.len() == member_count
                && matches!(schema.node(schema.resolve(*null)), Node::Null) =>
        {
            Some((true, members))
        }
        _ => None,
    }
}

impl ColumnEncoder {
    fn new(logical_type: &LogicalType, schema: &Schema, node: NodeId) -> Result<Self> {
        if logical_type.id()? == LogicalTypeID::Union {
            return Self::union(logical_type, schema, node);
        }
        let (nullable, node) = non_null_branch(schema, node);
        let kind = match logical_type.id()? {
            LogicalTypeID::SqlNull => EncoderKind::Null,
            LogicalTypeID::Boolean => EncoderKind::Boolean,
            LogicalTypeID::Integer => EncoderKind::Integer,
            LogicalTypeID::BigInt => EncoderKind::BigInt,
            LogicalTypeID::Float => EncoderKind::Float,
            LogicalTypeID::Double => EncoderKind::Double,
            LogicalTypeID::Varchar => EncoderKind::Varchar,
            LogicalTypeID::Blob => EncoderKind::Blob,
            LogicalTypeID::Date => EncoderKind::Date,
            LogicalTypeID::Time => EncoderKind::Time,
            LogicalTypeID::Timestamp => EncoderKind::Timestamp,
            LogicalTypeID::TimestampMs => EncoderKind::TimestampMs,
            LogicalTypeID::TimestampNs => EncoderKind::TimestampNs,
            LogicalTypeID::TimestampTz => EncoderKind::TimestampTz,
            LogicalTypeID::TimestampTzNs => EncoderKind::TimestampTzNs,
            LogicalTypeID::Uuid => EncoderKind::Uuid,
            LogicalTypeID::Decimal => {
                let Node::Fixed {
                    size: fixed_size, ..
                } = schema.node(node)
                else {
                    return Err(schema_mismatch(logical_type));
                };
                let width = DecimalSignature::from_logical_type(logical_type)?.width;
                let size = decimal_fixed_size(width)?;
                if *fixed_size != size as i64 {
                    return Err(schema_mismatch(logical_type));
                }
                EncoderKind::Decimal { width, size }
            }
            LogicalTypeID::Struct => {
                let Node::Record { fields, .. } = schema.node(node) else {
                    return Err(schema_mismatch(logical_type));
                };
                let children = struct_children(logical_type)?
                    .into_iter()
                    .enumerate()
                    .filter(|(_, (name, _))| name != EMPTY_STRUCT_MARKER)
                    .collect::<Vec<_>>();
                if children.len() != fields.len() {
                    return Err(schema_mismatch(logical_type));
                }
                let encoders = children
                    .into_iter()
                    .zip(fields)
                    .map(|((index, (_, child_type)), field)| {
                        Ok((
                            index,
                            ColumnEncoder::new(&child_type, schema, field.schema)?,
                        ))
                    })
                    .collect::<Result<Vec<_>>>()?;
                EncoderKind::Struct(encoders)
            }
            LogicalTypeID::List => {
                let Node::Array { items, .. } = schema.node(node) else {
                    return Err(schema_mismatch(logical_type));
                };
                EncoderKind::List(Box::new(ColumnEncoder::new(
                    &type_param(logical_type, 0)?,
                    schema,
                    *items,
                )?))
            }
            LogicalTypeID::Map => {
                let Node::Array { items, .. } = schema.node(node) else {
                    return Err(schema_mismatch(logical_type));
                };
                let Node::Record { fields, .. } = schema.node(schema.resolve(*items)) else {
                    return Err(schema_mismatch(logical_type));
                };
                let [key, value] = fields.as_slice() else {
                    return Err(schema_mismatch(logical_type));
                };
                EncoderKind::Map {
                    key: Box::new(ColumnEncoder::new(
                        &type_param(logical_type, 0)?,
                        schema,
                        key.schema,
                    )?),
                    value: Box::new(ColumnEncoder::new(
                        &type_param(logical_type, 1)?,
                        schema,
                        value.schema,
                    )?),
                }
            }
            LogicalTypeID::Enum => {
                let Node::Enum { symbols, .. } = schema.node(node) else {
                    return Err(schema_mismatch(logical_type));
                };
                if symbols.len() != logical_type.get_params()?.len() {
                    return Err(schema_mismatch(logical_type));
                }
                EncoderKind::Enum(EnumIndexWidth::for_symbol_count(symbols.len()))
            }
            _ => return Err(schema_mismatch(logical_type)),
        };
        Ok(Self {
            type_name: logical_type.name()?.to_owned(),
            nullable,
            kind,
        })
    }

    fn union(logical_type: &LogicalType, schema: &Schema, node: NodeId) -> Result<Self> {
        let members = struct_children(logical_type)?;
        let (nullable, branches) = union_branches(schema, node, members.len())
            .ok_or_else(|| schema_mismatch(logical_type))?;
        let members = members
            .iter()
            .zip(branches)
            .map(|((_, member_type), branch)| ColumnEncoder::new(member_type, schema, *branch))
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            type_name: logical_type.name()?.to_owned(),
            nullable,
            kind: EncoderKind::Union(members),
        })
    }
}

/// Flattens every vector of the chunk, children included, so rows map to their physical slots one to one.
pub fn flatten_chunk(chunk: &mut DataChunkRef<'_>) -> Result<()> {
    for index in 0..chunk.col_count() {
        let mut vector = chunk.get_vector_at_mut::<Unknown>(index)?;
        vector.flatten()?;
        flatten_children(vector.children_mut())?;
    }
    Ok(())
}

fn flatten_children(children: Option<ChildrenMut<'_>>) -> Result<()> {
    let Some(mut children) = children else {
        return Ok(());
    };
    for index in 0..children.col_count() {
        let mut child = children.get_untyped_vector_at_mut(index)?;
        child.flatten()?;
        flatten_children(child.children_mut())?;
    }
    Ok(())
}

enum Values<'a> {
    Null,
    Boolean(VectorReader<'a, bool>),
    Integer(VectorReader<'a, i32>),
    BigInt(VectorReader<'a, i64>),
    Float(VectorReader<'a, f32>),
    Double(VectorReader<'a, f64>),
    Varchar(VectorReader<'a, UncheckedString>),
    Blob(VectorReader<'a, BlobValue>),
    Date(VectorReader<'a, DateValue>),
    Time(VectorReader<'a, TimeValue>),
    Timestamp(VectorReader<'a, TimestampValue>),
    TimestampMs(VectorReader<'a, TimestampMsValue>),
    TimestampNs(VectorReader<'a, TimestampNsValue>),
    TimestampTz(VectorReader<'a, TimestampTzValue>),
    TimestampTzNs(VectorReader<'a, TimestampTzNsValue>),
    Uuid(VectorReader<'a, UuidValueRaw>),
    ShortDecimal(VectorReader<'a, ShortDecimal>, usize),
    WordDecimal(VectorReader<'a, WordDecimal>, usize),
    LongDecimal(VectorReader<'a, LongDecimal>, usize),
    HugeDecimal(VectorReader<'a, HugeDecimal>, usize),
    Struct {
        rows: VectorReader<'a, Struct>,
        fields: Vec<ColumnReader<'a>>,
    },
    List {
        rows: VectorReader<'a, List<Unknown>>,
        items: Box<ColumnReader<'a>>,
    },
    Map {
        rows: VectorReader<'a, Map<Unknown, Unknown>>,
        entries: VectorReader<'a, Struct>,
        key: Box<ColumnReader<'a>>,
        value: Box<ColumnReader<'a>>,
    },
    Enum(VectorReader<'a, Unknown>, EnumIndexWidth),
    Union {
        rows: VectorReader<'a, Union>,
        tags: VectorReader<'a, u8>,
        members: Vec<ColumnReader<'a>>,
    },
}

/// The rows of one (flat) vector, read to be encoded with its column's encoder.
pub struct ColumnReader<'a> {
    encoder: &'a ColumnEncoder,
    values: Values<'a>,
}

fn reader<'a, T: VectorElement>(vector: &'a Vector<'a, Unknown>) -> Result<VectorReader<'a, T>> {
    vector.cast_ref::<T>()?.reader()
}

fn child<'a>(vector: &'a Vector<'a, Unknown>, index: usize) -> Result<&'a Vector<'a, Unknown>> {
    vector
        .children()
        .ok_or_else(|| internal("Nested vector has no children"))?
        .get_untyped_vector_at(index)
}

fn list_entry<'a, T>(rows: &VectorReader<'a, T>, row: usize) -> Result<Option<ListIndex>>
where
    T: VectorElement<Internal<'a> = ListIndex>,
{
    if rows.is_null(row)? {
        return Ok(None);
    }
    let physical = rows.selection().index(row);
    // SAFETY: the row is in bounds and not NULL, so its physical slot holds the ListIndex DuckDB stores for every
    // LIST and MAP row.
    Ok(Some(unsafe { *rows.as_ptr().add(physical) }))
}

fn huge_int<T: VectorElement>(values: &VectorReader<'_, T>, row: usize) -> Result<Option<i128>> {
    if values.is_null(row)? {
        return Ok(None);
    }
    let physical = values.selection().index(row);
    // DuckDB aligns 16 byte integers to 8 bytes only, so they are read as HugeInt, not i128.
    // SAFETY: the row is in bounds and not NULL, and UUID and wide DECIMAL vectors store a 16 byte integer with the
    // layout of HugeInt in every slot.
    let value = unsafe { *values.as_ptr().cast::<HugeInt>().add(physical) };
    Ok(Some(
        (i128::from(value.upper) << 64) | i128::from(value.lower),
    ))
}

fn enum_index(
    values: &VectorReader<'_, Unknown>,
    width: EnumIndexWidth,
    row: usize,
) -> Result<Option<u32>> {
    if values.is_null(row)? {
        return Ok(None);
    }
    let physical = values.selection().index(row);
    let data = values.as_ptr();
    // SAFETY: the row is in bounds and not NULL, and an ENUM vector stores the dictionary index of every slot as the
    // unsigned integer of `width`.
    let index = unsafe {
        match width {
            EnumIndexWidth::U8 => u32::from(*data.cast::<u8>().add(physical)),
            EnumIndexWidth::U16 => u32::from(*data.cast::<u16>().add(physical)),
            EnumIndexWidth::U32 => *data.cast::<u32>().add(physical),
        }
    };
    Ok(Some(index))
}

fn write_decimal(out: &mut Vec<u8>, value: i128, size: usize) {
    let bytes = value.to_be_bytes();
    out.extend_from_slice(&bytes[bytes.len() - size..]);
}

impl<'a> ColumnReader<'a> {
    /// Prepares reading `vector` with `encoder`.
    pub fn new(encoder: &'a ColumnEncoder, vector: &'a Vector<'a, Unknown>) -> Result<Self> {
        let values = match &encoder.kind {
            EncoderKind::Null => Values::Null,
            EncoderKind::Boolean => Values::Boolean(reader(vector)?),
            EncoderKind::Integer => Values::Integer(reader(vector)?),
            EncoderKind::BigInt => Values::BigInt(reader(vector)?),
            EncoderKind::Float => Values::Float(reader(vector)?),
            EncoderKind::Double => Values::Double(reader(vector)?),
            EncoderKind::Varchar => Values::Varchar(reader(vector)?),
            EncoderKind::Blob => Values::Blob(reader(vector)?),
            EncoderKind::Date => Values::Date(reader(vector)?),
            EncoderKind::Time => Values::Time(reader(vector)?),
            EncoderKind::Timestamp => Values::Timestamp(reader(vector)?),
            EncoderKind::TimestampMs => Values::TimestampMs(reader(vector)?),
            EncoderKind::TimestampNs => Values::TimestampNs(reader(vector)?),
            EncoderKind::TimestampTz => Values::TimestampTz(reader(vector)?),
            EncoderKind::TimestampTzNs => Values::TimestampTzNs(reader(vector)?),
            EncoderKind::Uuid => Values::Uuid(reader(vector)?),
            EncoderKind::Decimal { width, size } => match width {
                ..=4 => Values::ShortDecimal(reader(vector)?, *size),
                5..=9 => Values::WordDecimal(reader(vector)?, *size),
                10..=18 => Values::LongDecimal(reader(vector)?, *size),
                _ => Values::HugeDecimal(reader(vector)?, *size),
            },
            EncoderKind::Struct(fields) => Values::Struct {
                rows: reader(vector)?,
                fields: fields
                    .iter()
                    .map(|(index, field)| ColumnReader::new(field, child(vector, *index)?))
                    .collect::<Result<_>>()?,
            },
            EncoderKind::List(items) => Values::List {
                rows: reader(vector)?,
                items: Box::new(ColumnReader::new(items, child(vector, 0)?)?),
            },
            EncoderKind::Map { key, value } => {
                let entries = child(vector, 0)?;
                Values::Map {
                    rows: reader(vector)?,
                    entries: reader(entries)?,
                    key: Box::new(ColumnReader::new(key, child(entries, 0)?)?),
                    value: Box::new(ColumnReader::new(value, child(entries, 1)?)?),
                }
            }
            EncoderKind::Enum(width) => Values::Enum(vector.reader()?, *width),
            // A UNION vector holds its tags in the first child, and its members after it.
            EncoderKind::Union(members) => Values::Union {
                rows: reader(vector)?,
                tags: reader(child(vector, 0)?)?,
                members: members
                    .iter()
                    .enumerate()
                    .map(|(index, member)| ColumnReader::new(member, child(vector, index + 1)?))
                    .collect::<Result<_>>()?,
            },
        };
        Ok(Self { encoder, values })
    }

    fn write_null(&self, out: &mut Vec<u8>) -> Result<()> {
        if self.encoder.nullable {
            write_long(out, 0);
        } else if !matches!(self.encoder.kind, EncoderKind::Null) {
            return Err(non_nullable_error(&self.encoder.type_name));
        }
        Ok(())
    }

    fn write<V>(
        &self,
        value: Option<V>,
        out: &mut Vec<u8>,
        write: impl FnOnce(&mut Vec<u8>, V) -> Result<()>,
    ) -> Result<()> {
        let Some(value) = value else {
            return self.write_null(out);
        };
        if self.encoder.nullable {
            write_long(out, 1);
        }
        write(out, value)
    }

    /// Appends the encoding of `row`.
    pub fn encode(&self, row: usize, out: &mut Vec<u8>) -> Result<()> {
        match &self.values {
            Values::Null => self.write_null(out),
            Values::Boolean(values) => self.write(values.get(row)?, out, |out, value| {
                write_boolean(out, *value);
                Ok(())
            }),
            Values::Integer(values) => self.write(values.get(row)?, out, |out, value| {
                write_int(out, *value);
                Ok(())
            }),
            Values::BigInt(values) => self.write(values.get(row)?, out, |out, value| {
                write_long(out, *value);
                Ok(())
            }),
            Values::Float(values) => self.write(values.get(row)?, out, |out, value| {
                write_float(out, *value);
                Ok(())
            }),
            Values::Double(values) => self.write(values.get(row)?, out, |out, value| {
                write_double(out, *value);
                Ok(())
            }),
            Values::Varchar(values) => self.write(values.get(row)?, out, |out, value| {
                write_bytes(out, value.as_bytes());
                Ok(())
            }),
            Values::Blob(values) => self.write(values.get(row)?, out, |out, value| {
                write_bytes(out, value);
                Ok(())
            }),
            Values::Date(values) => self.write(values.get(row)?, out, |out, value| {
                write_int(out, value.0);
                Ok(())
            }),
            Values::Time(values) => self.write(values.get(row)?, out, |out, value| {
                write_long(out, value.0);
                Ok(())
            }),
            Values::Timestamp(values) => self.write(values.get(row)?, out, |out, value| {
                write_long(out, value.0);
                Ok(())
            }),
            Values::TimestampMs(values) => self.write(values.get(row)?, out, |out, value| {
                write_long(out, value.0);
                Ok(())
            }),
            Values::TimestampNs(values) => self.write(values.get(row)?, out, |out, value| {
                write_long(out, value.0);
                Ok(())
            }),
            Values::TimestampTz(values) => self.write(values.get(row)?, out, |out, value| {
                write_long(out, value.0);
                Ok(())
            }),
            Values::TimestampTzNs(values) => self.write(values.get(row)?, out, |out, value| {
                write_long(out, value.0);
                Ok(())
            }),
            Values::Uuid(values) => self.write(huge_int(values, row)?, out, |out, value| {
                out.extend_from_slice(&UuidValueRaw(value).to_bytes());
                Ok(())
            }),
            Values::ShortDecimal(values, size) => {
                self.write(values.get(row)?, out, |out, value| {
                    write_decimal(out, value.0.into(), *size);
                    Ok(())
                })
            }
            Values::WordDecimal(values, size) => self.write(values.get(row)?, out, |out, value| {
                write_decimal(out, value.0.into(), *size);
                Ok(())
            }),
            Values::LongDecimal(values, size) => self.write(values.get(row)?, out, |out, value| {
                write_decimal(out, value.0.into(), *size);
                Ok(())
            }),
            Values::HugeDecimal(values, size) => {
                self.write(huge_int(values, row)?, out, |out, value| {
                    write_decimal(out, value, *size);
                    Ok(())
                })
            }
            Values::Struct { rows, fields } => self.write(rows.get(row)?, out, |out, ()| {
                for field in fields {
                    field.encode(row, out)?;
                }
                Ok(())
            }),
            Values::List { rows, items } => {
                self.write(list_entry(rows, row)?, out, |out, entry| {
                    let (offset, length) = entry_range(entry)?;
                    if length > 0 {
                        write_long(out, entry.length() as i64);
                    }
                    for item in offset..offset + length {
                        items.encode(item, out)?;
                    }
                    write_long(out, 0);
                    Ok(())
                })
            }
            Values::Map {
                rows,
                entries,
                key,
                value,
            } => self.write(list_entry(rows, row)?, out, |out, entry| {
                let (offset, length) = entry_range(entry)?;
                if length > 0 {
                    write_long(out, entry.length() as i64);
                }
                for item in offset..offset + length {
                    if entries.is_null(item)? {
                        return Err(non_nullable_error("STRUCT"));
                    }
                    key.encode(item, out)?;
                    value.encode(item, out)?;
                }
                write_long(out, 0);
                Ok(())
            }),
            Values::Enum(values, width) => {
                self.write(enum_index(values, *width, row)?, out, |out, index| {
                    write_long(out, i64::from(index));
                    Ok(())
                })
            }
            Values::Union {
                rows,
                tags,
                members,
            } => {
                let tag = match tags.get(row)? {
                    Some(tag) if !rows.is_null(row)? => *tag,
                    _ => return self.write_null(out),
                };
                let member = members.get(usize::from(tag)).ok_or_else(|| {
                    internal(format!(
                        "UNION tag {tag} beyond the {} members",
                        members.len()
                    ))
                })?;
                write_long(out, i64::from(tag) + i64::from(self.encoder.nullable));
                member.encode(row, out)
            }
        }
    }
}

fn entry_range(entry: ListIndex) -> Result<(usize, usize)> {
    let offset =
        usize::try_from(entry.offset()).map_err(|_| internal("LIST offset out of range"))?;
    let length =
        usize::try_from(entry.length()).map_err(|_| internal("LIST length out of range"))?;
    offset
        .checked_add(length)
        .ok_or_else(|| internal("LIST entry out of range"))?;
    Ok((offset, length))
}

fn non_nullable_error(type_name: &str) -> duckdb_neo::Error {
    invalid_input(format!(
        "Cannot insert NULL to non-nullable field of type {type_name}"
    ))
}
