//! Decoding Avro values into DuckDB vectors.
//!
//! The rows of a chunk are first decoded into a [`Column`] tree that mirrors the vectors they go into, and then copied
//! into the vectors in one pass. Decoding a chunk up front means the size of every LIST and MAP child is known before
//! its vector is sized, so no vector has to grow while it is being written.

use std::collections::HashMap;

use duckdb_neo::{
    Result,
    logical_type::LogicalTypeID,
    types::{BlobValue, List, Map, UncheckedString, UuidValueRaw},
    vector::{ChildVector, Unknown, Vector, VectorElement, VectorWriter},
};

use crate::avro::binary::Decoder;
use crate::avro::{AvroError, Result as AvroResult};
use crate::error::{internal, invalid_input, not_implemented, out_of_range};
use crate::types::{AvroKind, AvroType};

const MICROS_PER_MSEC: i64 = 1000;
const MAP_HASH_THRESHOLD: usize = 16;

fn prefixed<T>(result: AvroResult<T>, prefix: &str) -> AvroResult<T> {
    result.map_err(|error| error.prefixed(prefix))
}

fn read_integer(decoder: &mut Decoder<'_>, kind: AvroKind) -> AvroResult<i64> {
    match kind {
        AvroKind::Int => prefixed(decoder.read_int(), "Cannot read int value: ").map(i64::from),
        _ => prefixed(decoder.read_long(), "Cannot read long value: "),
    }
}

fn read_union_branch(decoder: &mut Decoder<'_>, branch_count: usize) -> AvroResult<usize> {
    let discriminant = prefixed(decoder.read_long(), "Cannot read union discriminant: ")?;
    usize::try_from(discriminant)
        .ok()
        .filter(|branch| *branch < branch_count)
        .ok_or_else(|| {
            AvroError::new(format!(
                "Invalid union discriminant value: ({discriminant})"
            ))
        })
}

struct BlockMessages {
    count: &'static str,
    size: &'static str,
}

const ARRAY_BLOCK: BlockMessages = BlockMessages {
    count: "Cannot read array block count: ",
    size: "Cannot read array block size: ",
};
const MAP_BLOCK: BlockMessages = BlockMessages {
    count: "Cannot read map block count: ",
    size: "Cannot read map block size: ",
};

// Items that take no bytes are not bounded by the data, so their number is capped instead.
const MAX_ZERO_WIDTH_ITEMS: u64 = 1 << 24;

// The byte size of a negative count is ignored like avro-c does, so that skipping a value reads as much as reading it.
fn read_block_count(
    decoder: &mut Decoder<'_>,
    messages: &BlockMessages,
    item_size: usize,
    items: &mut u64,
) -> AvroResult<u64> {
    let count = prefixed(decoder.read_long(), messages.count)?;
    if count < 0 {
        prefixed(decoder.read_long(), messages.size)?;
    }
    let count = count.unsigned_abs();
    *items = items.saturating_add(count);
    let fits = match decoder.remaining().checked_div(item_size) {
        Some(limit) => count <= limit as u64,
        None => *items <= MAX_ZERO_WIDTH_ITEMS,
    };
    if !fits {
        return Err(AvroError::new(
            "Avro block claims more items than its data holds",
        ));
    }
    Ok(count)
}

fn millis_to_micros(millis: i64) -> Result<i64> {
    millis.checked_mul(MICROS_PER_MSEC).ok_or_else(|| {
        out_of_range(format!(
            "Overflow in multiplication of INT64 ({millis} * {MICROS_PER_MSEC})!"
        ))
    })
}

// Keeps the lowest 128 bits of wider integers, like the C++ implementation did.
fn read_big_endian(bytes: &[u8]) -> i128 {
    let negative = bytes.first().is_some_and(|byte| byte & 0x80 != 0);
    let init = if negative { u128::MAX } else { 0 };
    bytes
        .iter()
        .fold(init, |value, byte| (value << 8) | u128::from(*byte)) as i128
}

fn invalid_string() -> duckdb_neo::Error {
    invalid_input("Avro file contains invalid unicode string")
}

#[derive(Debug, Clone, Copy)]
enum DecimalStorage {
    Short,
    Word,
    Long,
    Huge,
}

#[derive(Debug, Clone, Copy)]
enum EnumStorage {
    U8,
    U16,
    U32,
}

#[derive(Debug)]
enum Data {
    // A union without members that are not NULL still has a branch index to read.
    Null {
        union_branches: Option<usize>,
    },
    Boolean(Vec<bool>),
    Int(Vec<i32>),
    Long {
        millis: bool,
        values: Vec<i64>,
    },
    Float(Vec<f32>),
    Double(Vec<f64>),
    Uuid(Vec<[u64; 2]>),
    Decimal {
        storage: DecimalStorage,
        values: Vec<i128>,
    },
    Bytes {
        varchar: bool,
        data: Vec<u8>,
        ranges: Vec<(usize, usize)>,
    },
    Enum {
        size: usize,
        storage: EnumStorage,
        values: Vec<u32>,
    },
    Struct(Vec<Column>),
    List {
        map: bool,
        entries: Vec<[u64; 2]>,
        child: Box<Column>,
        item_size: usize,
    },
    Map {
        entries: Vec<[u64; 2]>,
        keys: Box<Column>,
        values: Box<Column>,
        entry_size: usize,
    },
    Union {
        members: Vec<Option<usize>>,
        tags: Vec<u8>,
        children: Vec<Column>,
    },
    Collapsed {
        branch: usize,
        branch_count: usize,
        child: Box<Column>,
    },
}

/// The values of one vector (and its children) for the rows decoded so far.
#[derive(Debug)]
pub struct Column {
    kind: AvroKind,
    type_id: LogicalTypeID,
    fixed_size: usize,
    validity: Vec<bool>,
    data: Data,
}

impl Column {
    /// Creates the column an Avro type is decoded into.
    pub fn new(avro_type: &AvroType) -> Result<Column> {
        let data = match avro_type.type_id {
            LogicalTypeID::SqlNull => Data::Null {
                union_branches: None,
            },
            LogicalTypeID::Boolean => Data::Boolean(Vec::new()),
            LogicalTypeID::Date | LogicalTypeID::Integer => Data::Int(Vec::new()),
            LogicalTypeID::Time
            | LogicalTypeID::Timestamp
            | LogicalTypeID::TimestampTz
            | LogicalTypeID::TimestampNs
            | LogicalTypeID::TimestampTzNs
            | LogicalTypeID::BigInt => Data::Long {
                millis: avro_type.is_millis,
                values: Vec::new(),
            },
            LogicalTypeID::Float => Data::Float(Vec::new()),
            LogicalTypeID::Double => Data::Double(Vec::new()),
            LogicalTypeID::Uuid => Data::Uuid(Vec::new()),
            LogicalTypeID::Decimal => Data::Decimal {
                storage: match avro_type.decimal_width {
                    0..=4 => DecimalStorage::Short,
                    5..=9 => DecimalStorage::Word,
                    10..=18 => DecimalStorage::Long,
                    19..=38 => DecimalStorage::Huge,
                    _ => return Err(not_implemented("Unsupported decimal physical type")),
                },
                values: Vec::new(),
            },
            LogicalTypeID::Blob => {
                if !matches!(avro_type.kind, AvroKind::Fixed | AvroKind::Bytes) {
                    return Err(not_implemented("Unknown Avro blob type"));
                }
                Data::Bytes {
                    varchar: false,
                    data: Vec::new(),
                    ranges: Vec::new(),
                }
            }
            LogicalTypeID::Varchar => Data::Bytes {
                varchar: true,
                data: Vec::new(),
                ranges: Vec::new(),
            },
            LogicalTypeID::Enum => {
                let size = avro_type.enum_symbols.len();
                Data::Enum {
                    size,
                    storage: if size <= usize::from(u8::MAX) {
                        EnumStorage::U8
                    } else if size <= usize::from(u16::MAX) {
                        EnumStorage::U16
                    } else {
                        EnumStorage::U32
                    },
                    values: Vec::new(),
                }
            }
            LogicalTypeID::Struct => Data::Struct(
                avro_type
                    .children
                    .iter()
                    .map(|(_, child)| Column::new(child))
                    .collect::<Result<_>>()?,
            ),
            LogicalTypeID::Map | LogicalTypeID::List if avro_type.kind == AvroKind::Array => {
                Data::List {
                    map: avro_type.type_id == LogicalTypeID::Map,
                    entries: Vec::new(),
                    child: Box::new(Column::new(&avro_type.children[0].1)?),
                    item_size: avro_type.children[0].1.min_size,
                }
            }
            LogicalTypeID::Map => Data::Map {
                entries: Vec::new(),
                keys: Box::new(Column::new(&avro_type.children[0].1)?),
                values: Box::new(Column::new(&avro_type.children[1].1)?),
                entry_size: 1 + avro_type.children[1].1.min_size,
            },
            LogicalTypeID::Union => match avro_type.non_null_member_count() {
                0 => Data::Null {
                    union_branches: Some(avro_type.children.len()),
                },
                1 => {
                    let branch = avro_type.single_member_branch().expect("one member");
                    Data::Collapsed {
                        branch,
                        branch_count: avro_type.children.len(),
                        child: Box::new(Column::new(&avro_type.children[branch].1)?),
                    }
                }
                _ => {
                    let mut children = Vec::new();
                    for (branch, member) in avro_type.union_members.iter().enumerate() {
                        if member.is_some() {
                            children.push(Column::new(&avro_type.children[branch].1)?);
                        }
                    }
                    Data::Union {
                        members: avro_type.union_members.clone(),
                        tags: Vec::new(),
                        children,
                    }
                }
            },
            other => {
                return Err(not_implemented(format!(
                    "Reading Avro values as {other:?} is not supported"
                )));
            }
        };
        let type_id = match data {
            Data::Null { .. } => LogicalTypeID::SqlNull,
            _ => avro_type.type_id,
        };
        Ok(Column {
            kind: avro_type.kind,
            type_id,
            fixed_size: avro_type.fixed_size,
            validity: Vec::new(),
            data,
        })
    }

    /// The number of rows decoded.
    pub fn len(&self) -> usize {
        match &self.data {
            Data::Collapsed { child, .. } => child.len(),
            _ => self.validity.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Drops every decoded row, keeping the allocations.
    pub fn clear(&mut self) {
        self.validity.clear();
        match &mut self.data {
            Data::Null { .. } => {}
            Data::Boolean(values) => values.clear(),
            Data::Int(values) => values.clear(),
            Data::Long { values, .. } => values.clear(),
            Data::Float(values) => values.clear(),
            Data::Double(values) => values.clear(),
            Data::Uuid(values) => values.clear(),
            Data::Decimal { values, .. } => values.clear(),
            Data::Bytes { data, ranges, .. } => {
                data.clear();
                ranges.clear();
            }
            Data::Enum { values, .. } => values.clear(),
            Data::Struct(fields) => fields.iter_mut().for_each(Column::clear),
            Data::List { entries, child, .. } => {
                entries.clear();
                child.clear();
            }
            Data::Map {
                entries,
                keys,
                values,
                ..
            } => {
                entries.clear();
                keys.clear();
                values.clear();
            }
            Data::Union { tags, children, .. } => {
                tags.clear();
                children.iter_mut().for_each(Column::clear);
            }
            Data::Collapsed { child, .. } => child.clear(),
        }
    }

    /// Appends a NULL row; the children of a NULL STRUCT or UNION are NULL as well.
    pub fn push_null(&mut self) {
        self.validity.push(false);
        match &mut self.data {
            Data::Null { .. } => {}
            Data::Boolean(values) => values.push(false),
            Data::Int(values) => values.push(0),
            Data::Long { values, .. } => values.push(0),
            Data::Float(values) => values.push(0.0),
            Data::Double(values) => values.push(0.0),
            Data::Uuid(values) => values.push([0; 2]),
            Data::Decimal { values, .. } => values.push(0),
            Data::Bytes { data, ranges, .. } => ranges.push((data.len(), 0)),
            Data::Enum { values, .. } => values.push(0),
            Data::Struct(fields) => fields.iter_mut().for_each(Column::push_null),
            Data::List { entries, child, .. } => entries.push([child.len() as u64, 0]),
            Data::Map { entries, keys, .. } => entries.push([keys.len() as u64, 0]),
            Data::Union { tags, children, .. } => {
                tags.push(0);
                children.iter_mut().for_each(Column::push_null);
            }
            Data::Collapsed { child, .. } => {
                self.validity.pop();
                child.push_null();
            }
        }
    }

    /// Decodes one value and appends it as a row.
    pub fn read(&mut self, decoder: &mut Decoder<'_>) -> Result<()> {
        let kind = self.kind;
        let fixed_size = self.fixed_size;
        match &mut self.data {
            Data::Null { union_branches } => {
                if let Some(branch_count) = *union_branches {
                    read_union_branch(decoder, branch_count)?;
                }
                self.validity.push(false);
                return Ok(());
            }
            Data::Boolean(values) => values.push(prefixed(
                decoder.read_boolean(),
                "Cannot read boolean value: ",
            )?),
            Data::Int(values) => {
                let value = read_integer(decoder, kind)?;
                values.push(
                    i32::try_from(value)
                        .map_err(|_| AvroError::new("Varint out of range for int type"))?,
                );
            }
            Data::Long { millis, values } => {
                let value = read_integer(decoder, kind)?;
                values.push(if *millis {
                    millis_to_micros(value)?
                } else {
                    value
                });
            }
            Data::Float(values) => {
                values.push(prefixed(decoder.read_float(), "Cannot read float value: ")?)
            }
            Data::Double(values) => values.push(prefixed(
                decoder.read_double(),
                "Cannot read double value: ",
            )?),
            Data::Uuid(values) => {
                let bytes: [u8; 16] =
                    prefixed(decoder.read_fixed(16), "Cannot read fixed value: ")?
                        .try_into()
                        .map_err(|_| internal("UUID is not 16 bytes"))?;
                let value = UuidValueRaw::from_bytes(bytes).0;
                values.push([value as u64, (value >> 64) as u64]);
            }
            Data::Decimal { values, .. } => {
                let bytes = if kind == AvroKind::Fixed {
                    prefixed(decoder.read_fixed(fixed_size), "Cannot read fixed value: ")?
                } else {
                    prefixed(decoder.read_bytes(), "Cannot read bytes value: ")?
                };
                values.push(read_big_endian(bytes));
            }
            Data::Bytes {
                varchar,
                data,
                ranges,
            } => {
                let bytes = match kind {
                    AvroKind::Fixed => {
                        prefixed(decoder.read_fixed(fixed_size), "Cannot read fixed value: ")?
                    }
                    AvroKind::String => {
                        prefixed(decoder.read_bytes(), "Cannot read string value: ")?
                    }
                    _ => prefixed(decoder.read_bytes(), "Cannot read bytes value: ")?,
                };
                if *varchar && std::str::from_utf8(bytes).is_err() {
                    return Err(invalid_string());
                }
                ranges.push((data.len(), bytes.len()));
                data.extend_from_slice(bytes);
            }
            Data::Enum { size, values, .. } => {
                let value = prefixed(decoder.read_long(), "Cannot read enum value: ")? as i32;
                let value = usize::try_from(value)
                    .ok()
                    .filter(|value| value < size)
                    .ok_or_else(|| invalid_input("Enum value out of range"))?;
                values.push(value as u32);
            }
            Data::Struct(fields) => {
                for field in fields {
                    field.read(decoder)?;
                }
            }
            Data::List {
                entries,
                child,
                item_size,
                ..
            } => {
                let offset = child.len();
                let mut items = 0;
                loop {
                    let count = read_block_count(decoder, &ARRAY_BLOCK, *item_size, &mut items)?;
                    if count == 0 {
                        break;
                    }
                    for _ in 0..count {
                        child.read(decoder)?;
                    }
                }
                entries.push([offset as u64, (child.len() - offset) as u64]);
            }
            Data::Map {
                entries,
                keys,
                values,
                entry_size,
            } => {
                let offset = keys.len();
                read_map(decoder, offset, keys, values, *entry_size)?;
                entries.push([offset as u64, (keys.len() - offset) as u64]);
            }
            Data::Union {
                members,
                tags,
                children,
            } => {
                let branch = read_union_branch(decoder, members.len())?;
                let Some(member) = members[branch] else {
                    tags.push(0);
                    children.iter_mut().for_each(Column::push_null);
                    self.validity.push(false);
                    return Ok(());
                };
                tags.push(member as u8);
                for (index, child) in children.iter_mut().enumerate() {
                    if index == member {
                        child.read(decoder)?;
                    } else {
                        child.push_null();
                    }
                }
            }
            Data::Collapsed {
                branch,
                branch_count,
                child,
            } => {
                if read_union_branch(decoder, *branch_count)? == *branch {
                    child.read(decoder)?;
                } else {
                    child.push_null();
                }
                return Ok(());
            }
        }
        self.validity.push(true);
        Ok(())
    }

    fn move_row(&mut self, from: usize, to: usize) {
        if let Data::Collapsed { child, .. } = &mut self.data {
            child.move_row(from, to);
            return;
        }
        self.validity[to] = self.validity[from];
        match &mut self.data {
            Data::Null { .. } | Data::Collapsed { .. } => {}
            Data::Boolean(values) => values[to] = values[from],
            Data::Int(values) => values[to] = values[from],
            Data::Long { values, .. } => values[to] = values[from],
            Data::Float(values) => values[to] = values[from],
            Data::Double(values) => values[to] = values[from],
            Data::Uuid(values) => values[to] = values[from],
            Data::Decimal { values, .. } => values[to] = values[from],
            Data::Bytes { ranges, .. } => ranges[to] = ranges[from],
            Data::Enum { values, .. } => values[to] = values[from],
            Data::Struct(fields) => fields.iter_mut().for_each(|field| field.move_row(from, to)),
            Data::List { entries, .. } | Data::Map { entries, .. } => entries[to] = entries[from],
            Data::Union { tags, children, .. } => {
                tags[to] = tags[from];
                children
                    .iter_mut()
                    .for_each(|child| child.move_row(from, to));
            }
        }
    }

    // The children of dropped rows stay, unreferenced.
    fn truncate(&mut self, len: usize) {
        if let Data::Collapsed { child, .. } = &mut self.data {
            child.truncate(len);
            return;
        }
        self.validity.truncate(len);
        match &mut self.data {
            Data::Null { .. } | Data::Collapsed { .. } => {}
            Data::Boolean(values) => values.truncate(len),
            Data::Int(values) => values.truncate(len),
            Data::Long { values, .. } => values.truncate(len),
            Data::Float(values) => values.truncate(len),
            Data::Double(values) => values.truncate(len),
            Data::Uuid(values) => values.truncate(len),
            Data::Decimal { values, .. } => values.truncate(len),
            Data::Bytes { ranges, .. } => ranges.truncate(len),
            Data::Enum { values, .. } => values.truncate(len),
            Data::Struct(fields) => fields.iter_mut().for_each(|field| field.truncate(len)),
            Data::List { entries, .. } | Data::Map { entries, .. } => entries.truncate(len),
            Data::Union { tags, children, .. } => {
                tags.truncate(len);
                children.iter_mut().for_each(|child| child.truncate(len));
            }
        }
    }

    fn key(&self, row: usize) -> &[u8] {
        match &self.data {
            Data::Bytes { data, ranges, .. } => {
                let (start, len) = ranges[row];
                &data[start..start + len]
            }
            _ => &[],
        }
    }

    fn push_key(&mut self, key: &[u8]) {
        if let Data::Bytes { data, ranges, .. } = &mut self.data {
            ranges.push((data.len(), key.len()));
            data.extend_from_slice(key);
            self.validity.push(true);
        }
    }
}

// A key that repeats keeps its first position and its last value, like it does in avro-c's in-memory map.
fn read_map(
    decoder: &mut Decoder<'_>,
    offset: usize,
    keys: &mut Column,
    values: &mut Column,
    entry_size: usize,
) -> Result<()> {
    let mut index: Option<HashMap<Vec<u8>, usize>> = None;
    let mut items = 0;
    loop {
        let count = read_block_count(decoder, &MAP_BLOCK, entry_size, &mut items)?;
        if count == 0 {
            return Ok(());
        }
        for _ in 0..count {
            let raw_key = prefixed(decoder.read_bytes(), "Cannot read map key: ")?;
            // Keys are C strings to avro-c, which ends them at the first NUL
            let key = raw_key.split(|byte| *byte == 0).next().unwrap_or_default();
            if std::str::from_utf8(key).is_err() {
                return Err(invalid_string());
            }
            let entries = keys.len() - offset;
            if entries == MAP_HASH_THRESHOLD && index.is_none() {
                index = Some(
                    (offset..keys.len())
                        .map(|row| (keys.key(row).to_vec(), row))
                        .collect(),
                );
            }
            let existing = match &index {
                Some(index) => index.get(key).copied(),
                None => (offset..keys.len()).find(|row| keys.key(*row) == key),
            };
            match existing {
                Some(row) => {
                    let end = values.len();
                    values.read(decoder)?;
                    values.move_row(end, row);
                    values.truncate(end);
                }
                None => {
                    if let Some(index) = &mut index {
                        index.insert(key.to_vec(), keys.len());
                    }
                    keys.push_key(key);
                    values.read(decoder)?;
                }
            }
        }
    }
}

/// Skips a value that is not read.
pub fn skip(avro_type: &AvroType, decoder: &mut Decoder<'_>) -> Result<()> {
    match avro_type.kind {
        AvroKind::Null => {}
        AvroKind::Boolean => {
            prefixed(decoder.read_boolean(), "Cannot read boolean value: ")?;
        }
        AvroKind::Int => {
            prefixed(decoder.read_int(), "Cannot read int value: ")?;
        }
        AvroKind::Long | AvroKind::Enum => {
            prefixed(decoder.read_long(), "Cannot read long value: ")?;
        }
        AvroKind::Float => decoder.skip(4)?,
        AvroKind::Double => decoder.skip(8)?,
        AvroKind::Bytes | AvroKind::String => {
            prefixed(decoder.read_bytes(), "Cannot read bytes value: ")?;
        }
        AvroKind::Fixed => prefixed(
            decoder.skip(avro_type.fixed_size),
            "Cannot read fixed value: ",
        )?,
        AvroKind::Record => {
            for (_, child) in &avro_type.children {
                skip(child, decoder)?;
            }
        }
        AvroKind::Array => {
            let item = &avro_type.children[0].1;
            let mut items = 0;
            loop {
                let count = read_block_count(decoder, &ARRAY_BLOCK, item.min_size, &mut items)?;
                if count == 0 {
                    break;
                }
                for _ in 0..count {
                    skip(item, decoder)?;
                }
            }
        }
        AvroKind::Map => {
            let value = &avro_type.children[1].1;
            let mut items = 0;
            loop {
                let count = read_block_count(decoder, &MAP_BLOCK, 1 + value.min_size, &mut items)?;
                if count == 0 {
                    break;
                }
                for _ in 0..count {
                    prefixed(decoder.read_bytes(), "Cannot read map key: ")?;
                    skip(value, decoder)?;
                }
            }
        }
        AvroKind::Union => {
            let branch = read_union_branch(decoder, avro_type.children.len())?;
            skip(&avro_type.children[branch].1, decoder)?;
        }
    }
    Ok(())
}

fn check_len(expected: usize, actual: usize) -> Result<()> {
    if expected != actual {
        return Err(internal(format!(
            "Avro column has {actual} values for a vector of {expected} rows"
        )));
    }
    Ok(())
}

fn write_validity<E: VectorElement>(
    writer: &mut VectorWriter<'_, E>,
    validity: &[bool],
) -> Result<()> {
    check_len(writer.len(), validity.len())?;
    // SAFETY: every row the validity marks valid is written by the caller before the vector is read.
    unsafe {
        writer.fill_validity(true)?;
        for (row, valid) in validity.iter().enumerate() {
            if !valid {
                writer.set_row_validity(row, false)?;
            }
        }
    }
    Ok(())
}

fn write_values<E: VectorElement, T: Copy>(
    writer: &mut VectorWriter<'_, E>,
    values: &[T],
    validity: &[bool],
) -> Result<()> {
    check_len(writer.len(), values.len())?;
    let data = writer.as_mut_ptr().cast::<T>();
    if data.is_null() {
        return Err(internal("Avro column vector has no data"));
    }
    // SAFETY: the vector is flat and writable, holds `values.len()` rows, and `T` is the physical type of its logical
    // type, which the column was created for.
    unsafe { std::ptr::copy_nonoverlapping(values.as_ptr(), data, values.len()) };
    write_validity(writer, validity)
}

fn convert<T: Copy, U>(values: &[T], f: impl Fn(T) -> U) -> Vec<U> {
    values.iter().map(|value| f(*value)).collect()
}

macro_rules! fill_vector {
    ($name:ident, $vector:ident) => {
        /// Writes the rows of `column` into `vector`, which already has as many rows.
        pub fn $name(vector: $vector<'_, Unknown>, column: &Column) -> Result<()> {
            let mut vector = vector;
            let validity = column.validity.as_slice();
            match &column.data {
                Data::Collapsed { child, .. } => return $name(vector, child),
                _ if vector.logical_type().id()? != column.type_id => {
                    return Err(internal(format!(
                        "Avro column of type {:?} for a vector of type {}",
                        column.type_id,
                        vector.logical_type()
                    )));
                }
                Data::Null { .. } => {
                    let mut writer = vector.writer()?;
                    check_len(writer.len(), validity.len())?;
                    // SAFETY: NULL rows need no data.
                    unsafe { writer.fill_validity(false)? };
                }
                Data::Boolean(values) => write_values(&mut vector.writer()?, values, validity)?,
                Data::Int(values) => write_values(&mut vector.writer()?, values, validity)?,
                Data::Long { values, .. } => write_values(&mut vector.writer()?, values, validity)?,
                Data::Float(values) => write_values(&mut vector.writer()?, values, validity)?,
                Data::Double(values) => write_values(&mut vector.writer()?, values, validity)?,
                Data::Uuid(values) => write_values(&mut vector.writer()?, values, validity)?,
                Data::Decimal { storage, values } => {
                    let mut writer = vector.writer()?;
                    match storage {
                        DecimalStorage::Short => {
                            write_values(&mut writer, &convert(values, |v| v as i16), validity)?
                        }
                        DecimalStorage::Word => {
                            write_values(&mut writer, &convert(values, |v| v as i32), validity)?
                        }
                        DecimalStorage::Long => {
                            write_values(&mut writer, &convert(values, |v| v as i64), validity)?
                        }
                        DecimalStorage::Huge => write_values(
                            &mut writer,
                            &convert(values, |v| [v as u64, (v >> 64) as u64]),
                            validity,
                        )?,
                    }
                }
                Data::Enum {
                    storage, values, ..
                } => {
                    let mut writer = vector.writer()?;
                    match storage {
                        EnumStorage::U8 => {
                            write_values(&mut writer, &convert(values, |v| v as u8), validity)?
                        }
                        EnumStorage::U16 => {
                            write_values(&mut writer, &convert(values, |v| v as u16), validity)?
                        }
                        EnumStorage::U32 => write_values(&mut writer, values, validity)?,
                    }
                }
                Data::Bytes {
                    varchar: true,
                    data,
                    ranges,
                } => {
                    let mut vector = vector.cast::<UncheckedString>()?;
                    let mut writer = vector.writer()?;
                    check_len(writer.len(), ranges.len())?;
                    for (row, (start, len)) in ranges.iter().enumerate() {
                        let text = validity[row].then(|| &data[*start..start + len]);
                        // SAFETY: only valid UTF-8 is decoded into a VARCHAR column.
                        writer.set(
                            row,
                            text.map(|text| unsafe { std::str::from_utf8_unchecked(text) }),
                        )?;
                    }
                }
                Data::Bytes {
                    varchar: false,
                    data,
                    ranges,
                } => {
                    let mut vector = vector.cast::<BlobValue>()?;
                    let mut writer = vector.writer()?;
                    check_len(writer.len(), ranges.len())?;
                    for (row, (start, len)) in ranges.iter().enumerate() {
                        writer.set(row, validity[row].then(|| &data[*start..start + len]))?;
                    }
                }
                Data::Struct(fields) => {
                    write_validity(&mut vector.writer()?, validity)?;
                    if let Some(mut children) = vector.children_mut() {
                        for (index, field) in fields.iter().enumerate() {
                            fill_child(children.get_untyped_vector_at_mut(index)?, field)?;
                        }
                    }
                }
                Data::List {
                    map: false,
                    entries,
                    child,
                    ..
                } => {
                    let mut vector = vector.cast::<List<Unknown>>()?;
                    // SAFETY: every element of the child is written below.
                    unsafe { vector.set_child_size(child.len())? };
                    write_values(&mut vector.writer()?, entries, validity)?;
                    let mut children = vector.children_mut().expect("LIST has a child");
                    fill_child(children.get_untyped_vector_at_mut(0)?, child)?;
                }
                Data::List {
                    map: true,
                    entries,
                    child,
                    ..
                } => {
                    let mut vector = vector.cast::<Map<Unknown, Unknown>>()?;
                    // SAFETY: every entry of the child is written below.
                    unsafe { vector.set_child_size(child.len())? };
                    write_values(&mut vector.writer()?, entries, validity)?;
                    let mut children = vector.children_mut().expect("MAP has entries");
                    fill_child(children.get_untyped_vector_at_mut(0)?, child)?;
                }
                Data::Map {
                    entries,
                    keys,
                    values,
                    ..
                } => {
                    let mut vector = vector.cast::<Map<Unknown, Unknown>>()?;
                    // SAFETY: every entry of the child is written below.
                    unsafe { vector.set_child_size(keys.len())? };
                    write_values(&mut vector.writer()?, entries, validity)?;
                    let mut children = vector.children_mut().expect("MAP has entries");
                    let mut entries = children.get_untyped_vector_at_mut(0)?;
                    let mut writer = entries.writer()?;
                    check_len(writer.len(), keys.len())?;
                    // SAFETY: every entry has a key and a value, written below.
                    unsafe { writer.fill_validity(true)? };
                    drop(writer);
                    let mut fields = entries
                        .children_mut()
                        .expect("MAP entries have a key and a value");
                    fill_child(fields.get_untyped_vector_at_mut(0)?, keys)?;
                    fill_child(fields.get_untyped_vector_at_mut(1)?, values)?;
                }
                Data::Union { tags, children, .. } => {
                    write_validity(&mut vector.writer()?, validity)?;
                    let mut members = vector.children_mut().expect("UNION has a tag and members");
                    write_values(
                        &mut members.get_untyped_vector_at_mut(0)?.writer()?,
                        tags,
                        validity,
                    )?;
                    for (index, child) in children.iter().enumerate() {
                        fill_child(members.get_untyped_vector_at_mut(index + 1)?, child)?;
                    }
                }
            }
            Ok(())
        }
    };
}

fill_vector!(fill_vector, Vector);
fill_vector!(fill_child, ChildVector);
