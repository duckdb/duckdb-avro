//! The decoding tree of a scan: one [`Node`] per projected column, mirroring the [`Shape`] of its schema node.
//!
//! A node buffers the values of the rows of one output chunk (`decode` appends a value, `push_null` a NULL) and
//! [`Node::flush`] writes the buffers into the vector with one bulk write per vector: the engine is called once per
//! vector per chunk, never per row. The buffers keep their capacity across chunks ([`Node::clear`]).

use avro_core::{Decoder, Schema, SchemaId};
use duckdb_ext_api::sys::duckdb_v2_list_entry;
use duckdb_ext_api::vector::Element;
use duckdb_ext_api::{Error, Result, StringWriter, Vector};

use super::schema::{Column, Shape};

/// Microseconds per millisecond.
const MICROS_PER_MSEC: i64 = 1000;

/// The values of a chunk's rows for a vector of plain elements.
#[derive(Debug, Default)]
pub(crate) struct Prim<T> {
    values: Vec<T>,
    /// The rows that are NULL (their element is a placeholder).
    nulls: Vec<usize>,
}

impl<T: Element + Default> Prim<T> {
    fn new() -> Prim<T> {
        Prim { values: Vec::new(), nulls: Vec::new() }
    }

    #[inline]
    fn push(&mut self, value: T) {
        self.values.push(value);
    }

    #[inline]
    fn push_null(&mut self) {
        self.nulls.push(self.values.len());
        self.values.push(T::default());
    }

    fn len(&self) -> usize {
        self.values.len()
    }

    fn clear(&mut self) {
        self.values.clear();
        self.nulls.clear();
    }

    fn flush(&self, vector: &Vector) -> Result<()> {
        let count = self.values.len();
        vector.flat_data_mut::<T>(count)?.copy_from_slice(&self.values);
        set_nulls(vector, count, &self.nulls)
    }
}

/// Marks `nulls` NULL in the first `count` rows of `vector`.
fn set_nulls(vector: &Vector, count: usize, nulls: &[usize]) -> Result<()> {
    if nulls.is_empty() {
        return Ok(());
    }
    let validity = vector.flat_validity_mut(count)?;
    for &row in nulls {
        validity.set_invalid(row);
    }
    Ok(())
}

/// The values of a chunk's rows for a VARCHAR / BLOB vector: the bytes back to back, and where each row's end.
#[derive(Debug, Default)]
pub(crate) struct Strings {
    data: Vec<u8>,
    ends: Vec<usize>,
    nulls: Vec<usize>,
}

impl Strings {
    #[inline]
    fn push(&mut self, bytes: &[u8]) {
        self.data.extend_from_slice(bytes);
        self.ends.push(self.data.len());
    }

    #[inline]
    fn push_null(&mut self) {
        self.nulls.push(self.ends.len());
        self.ends.push(self.data.len());
    }

    fn len(&self) -> usize {
        self.ends.len()
    }

    fn clear(&mut self) {
        self.data.clear();
        self.ends.clear();
        self.nulls.clear();
    }

    fn flush(&self, vector: &Vector) -> Result<()> {
        let writer = StringWriter::new(vector, self.ends.len())?;
        let mut start = 0;
        for (row, &end) in self.ends.iter().enumerate() {
            writer.write(row, &self.data[start..end])?;
            start = end;
        }
        for &row in &self.nulls {
            writer.write_null(row);
        }
        Ok(())
    }
}

/// The unscaled values of a DECIMAL column, in the storage width of its precision.
#[derive(Debug)]
pub(crate) enum DecimalValues {
    I16(Prim<i16>),
    I32(Prim<i32>),
    I64(Prim<i64>),
    I128(Prim<i128>),
}

/// The symbol indexes of an ENUM column, in the storage width of its symbol count.
#[derive(Debug)]
pub(crate) enum EnumValues {
    U8(Prim<u8>),
    U16(Prim<u16>),
    U32(Prim<u32>),
}

/// The decoder and buffer of one column or nested field.
#[derive(Debug)]
pub(crate) enum Node {
    Null {
        id: SchemaId,
        len: usize,
    },
    Boolean(Prim<u8>),
    Int(Prim<i32>),
    Long(Prim<i64>),
    TimeMillis(Prim<i64>),
    TimestampMillis(Prim<i64>),
    Float(Prim<f32>),
    Double(Prim<f64>),
    Bytes(Strings),
    String(Strings),
    Fixed {
        size: usize,
        values: Strings,
    },
    Uuid(Prim<i128>),
    Decimal {
        fixed: Option<usize>,
        values: DecimalValues,
    },
    Enum {
        count: usize,
        values: EnumValues,
    },
    Struct {
        fields: Vec<Node>,
        len: usize,
        nulls: Vec<usize>,
    },
    List {
        entries: Vec<duckdb_v2_list_entry>,
        child: Box<Node>,
        nulls: Vec<usize>,
    },
    Map {
        entries: Vec<duckdb_v2_list_entry>,
        key: Box<Node>,
        value: Box<Node>,
        from_array: bool,
        nulls: Vec<usize>,
    },
    Union {
        branch_ids: Vec<SchemaId>,
        branch_members: Vec<Option<usize>>,
        members: Vec<Node>,
        tags: Vec<u8>,
        nulls: Vec<usize>,
    },
    Nullable {
        branch_ids: Vec<SchemaId>,
        branch: usize,
        child: Box<Node>,
    },
}

impl Node {
    /// The node reading `column`.
    pub(crate) fn new(column: &Column) -> Node {
        match &column.shape {
            Shape::Null(id) => Node::Null { id: *id, len: 0 },
            Shape::Boolean => Node::Boolean(Prim::new()),
            Shape::Int => Node::Int(Prim::new()),
            Shape::Long => Node::Long(Prim::new()),
            Shape::TimeMillis => Node::TimeMillis(Prim::new()),
            Shape::TimestampMillis => Node::TimestampMillis(Prim::new()),
            Shape::Float => Node::Float(Prim::new()),
            Shape::Double => Node::Double(Prim::new()),
            Shape::Bytes => Node::Bytes(Strings::default()),
            Shape::String => Node::String(Strings::default()),
            Shape::Fixed(size) => Node::Fixed { size: *size, values: Strings::default() },
            Shape::Uuid => Node::Uuid(Prim::new()),
            Shape::Decimal { fixed, width } => {
                let values = match width {
                    2 => DecimalValues::I16(Prim::new()),
                    4 => DecimalValues::I32(Prim::new()),
                    8 => DecimalValues::I64(Prim::new()),
                    _ => DecimalValues::I128(Prim::new()),
                };
                Node::Decimal { fixed: *fixed, values }
            }
            Shape::Enum { count, width } => {
                let values = match width {
                    1 => EnumValues::U8(Prim::new()),
                    2 => EnumValues::U16(Prim::new()),
                    _ => EnumValues::U32(Prim::new()),
                };
                Node::Enum { count: *count, values }
            }
            Shape::Struct(fields) => {
                Node::Struct { fields: fields.iter().map(Node::new).collect(), len: 0, nulls: Vec::new() }
            }
            Shape::List(element) => {
                Node::List { entries: Vec::new(), child: Box::new(Node::new(element)), nulls: Vec::new() }
            }
            Shape::Map { key, value, from_array } => Node::Map {
                entries: Vec::new(),
                key: Box::new(Node::new(key)),
                value: Box::new(Node::new(value)),
                from_array: *from_array,
                nulls: Vec::new(),
            },
            Shape::Union { branch_ids, branch_members, members } => Node::Union {
                branch_ids: branch_ids.clone(),
                branch_members: branch_members.clone(),
                members: members.iter().map(Node::new).collect(),
                tags: Vec::new(),
                nulls: Vec::new(),
            },
            Shape::Nullable { branch_ids, branch, child } => {
                Node::Nullable { branch_ids: branch_ids.clone(), branch: *branch, child: Box::new(Node::new(child)) }
            }
        }
    }

    /// The number of values buffered.
    pub(crate) fn len(&self) -> usize {
        match self {
            Node::Null { len, .. } => *len,
            Node::Boolean(p) => p.len(),
            Node::Int(p) => p.len(),
            Node::Long(p) | Node::TimeMillis(p) | Node::TimestampMillis(p) => p.len(),
            Node::Float(p) => p.len(),
            Node::Double(p) => p.len(),
            Node::Bytes(s) | Node::String(s) => s.len(),
            Node::Fixed { values, .. } => values.len(),
            Node::Uuid(p) => p.len(),
            Node::Decimal { values, .. } => match values {
                DecimalValues::I16(p) => p.len(),
                DecimalValues::I32(p) => p.len(),
                DecimalValues::I64(p) => p.len(),
                DecimalValues::I128(p) => p.len(),
            },
            Node::Enum { values, .. } => match values {
                EnumValues::U8(p) => p.len(),
                EnumValues::U16(p) => p.len(),
                EnumValues::U32(p) => p.len(),
            },
            Node::Struct { len, .. } => *len,
            Node::List { entries, .. } | Node::Map { entries, .. } => entries.len(),
            Node::Union { tags, .. } => tags.len(),
            Node::Nullable { child, .. } => child.len(),
        }
    }

    /// Forgets the buffered values (keeping the capacity).
    pub(crate) fn clear(&mut self) {
        match self {
            Node::Null { len, .. } => *len = 0,
            Node::Boolean(p) => p.clear(),
            Node::Int(p) => p.clear(),
            Node::Long(p) | Node::TimeMillis(p) | Node::TimestampMillis(p) => p.clear(),
            Node::Float(p) => p.clear(),
            Node::Double(p) => p.clear(),
            Node::Bytes(s) | Node::String(s) => s.clear(),
            Node::Fixed { values, .. } => values.clear(),
            Node::Uuid(p) => p.clear(),
            Node::Decimal { values, .. } => match values {
                DecimalValues::I16(p) => p.clear(),
                DecimalValues::I32(p) => p.clear(),
                DecimalValues::I64(p) => p.clear(),
                DecimalValues::I128(p) => p.clear(),
            },
            Node::Enum { values, .. } => match values {
                EnumValues::U8(p) => p.clear(),
                EnumValues::U16(p) => p.clear(),
                EnumValues::U32(p) => p.clear(),
            },
            Node::Struct { fields, len, nulls } => {
                fields.iter_mut().for_each(Node::clear);
                *len = 0;
                nulls.clear();
            }
            Node::List { entries, child, nulls } => {
                entries.clear();
                child.clear();
                nulls.clear();
            }
            Node::Map { entries, key, value, nulls, .. } => {
                entries.clear();
                key.clear();
                value.clear();
                nulls.clear();
            }
            Node::Union { members, tags, nulls, .. } => {
                members.iter_mut().for_each(Node::clear);
                tags.clear();
                nulls.clear();
            }
            Node::Nullable { child, .. } => child.clear(),
        }
    }

    /// Appends a NULL value (recursively: the children of a NULL STRUCT or UNION row are NULL too, as the engine's
    /// `FlatVector::SetNull` makes them).
    pub(crate) fn push_null(&mut self) {
        match self {
            Node::Null { len, .. } => *len += 1,
            Node::Boolean(p) => p.push_null(),
            Node::Int(p) => p.push_null(),
            Node::Long(p) | Node::TimeMillis(p) | Node::TimestampMillis(p) => p.push_null(),
            Node::Float(p) => p.push_null(),
            Node::Double(p) => p.push_null(),
            Node::Bytes(s) | Node::String(s) => s.push_null(),
            Node::Fixed { values, .. } => values.push_null(),
            Node::Uuid(p) => p.push_null(),
            Node::Decimal { values, .. } => match values {
                DecimalValues::I16(p) => p.push_null(),
                DecimalValues::I32(p) => p.push_null(),
                DecimalValues::I64(p) => p.push_null(),
                DecimalValues::I128(p) => p.push_null(),
            },
            Node::Enum { values, .. } => match values {
                EnumValues::U8(p) => p.push_null(),
                EnumValues::U16(p) => p.push_null(),
                EnumValues::U32(p) => p.push_null(),
            },
            Node::Struct { fields, len, nulls } => {
                nulls.push(*len);
                *len += 1;
                fields.iter_mut().for_each(Node::push_null);
            }
            Node::List { entries, child, nulls } => {
                nulls.push(entries.len());
                entries.push(duckdb_v2_list_entry { offset: child.len() as u64, length: 0 });
            }
            Node::Map { entries, key, nulls, .. } => {
                nulls.push(entries.len());
                entries.push(duckdb_v2_list_entry { offset: key.len() as u64, length: 0 });
            }
            Node::Union { members, tags, nulls, .. } => {
                nulls.push(tags.len());
                tags.push(0);
                members.iter_mut().for_each(Node::push_null);
            }
            Node::Nullable { child, .. } => child.push_null(),
        }
    }

    /// Decodes one value of the node's schema from `dec` and appends it.
    pub(crate) fn decode(&mut self, dec: &mut Decoder<'_>, schema: &Schema) -> Result<()> {
        match self {
            Node::Null { id, len } => {
                dec.skip(schema, *id).or_err()?;
                *len += 1;
            }
            Node::Boolean(p) => p.push(u8::from(dec.read_boolean().or_err()?)),
            Node::Int(p) => p.push(dec.read_int().or_err()?),
            Node::Long(p) => p.push(dec.read_long().or_err()?),
            Node::TimeMillis(p) => p.push(i64::from(dec.read_int().or_err()?) * MICROS_PER_MSEC),
            Node::TimestampMillis(p) => {
                let millis = dec.read_long().or_err()?;
                let micros = millis.checked_mul(MICROS_PER_MSEC).ok_or_else(|| {
                    Error::out_of_range(format!("Overflow in multiplication of INT64 ({millis} * {MICROS_PER_MSEC})!"))
                })?;
                p.push(micros);
            }
            Node::Float(p) => p.push(dec.read_float().or_err()?),
            Node::Double(p) => p.push(dec.read_double().or_err()?),
            Node::Bytes(s) => s.push(dec.read_bytes().or_err()?),
            Node::String(s) => s.push(dec.read_string().or_err()?.as_bytes()),
            Node::Fixed { size, values } => values.push(dec.read_fixed(*size).or_err()?),
            Node::Uuid(p) => {
                let raw = dec.read_fixed(16).or_err()?;
                let mut upper = 0u64;
                let mut lower = 0u64;
                for i in 0..8 {
                    upper = (upper << 8) | u64::from(raw[i]);
                    lower = (lower << 8) | u64::from(raw[8 + i]);
                }
                // the engine's UUID order: the sign bit of the upper half flipped
                let upper = upper ^ (1u64 << 63);
                p.push(((upper as i128) << 64) | lower as i128);
            }
            Node::Decimal { fixed, values } => {
                let raw = match fixed {
                    Some(size) => dec.read_fixed(*size).or_err()?,
                    None => dec.read_bytes().or_err()?,
                };
                let unscaled = read_decimal(raw);
                match values {
                    DecimalValues::I16(p) => p.push(unscaled as i16),
                    DecimalValues::I32(p) => p.push(unscaled as i32),
                    DecimalValues::I64(p) => p.push(unscaled as i64),
                    DecimalValues::I128(p) => p.push(unscaled),
                }
            }
            Node::Enum { count, values } => {
                let index = dec.read_enum(*count).or_err()?;
                match values {
                    EnumValues::U8(p) => p.push(index as u8),
                    EnumValues::U16(p) => p.push(index as u16),
                    EnumValues::U32(p) => p.push(index as u32),
                }
            }
            Node::Struct { fields, len, .. } => {
                for field in fields.iter_mut() {
                    field.decode(dec, schema)?;
                }
                *len += 1;
            }
            Node::List { entries, child, .. } => {
                let offset = child.len() as u64;
                let mut length = 0u64;
                loop {
                    let block = dec.read_block_count().or_err()?;
                    if block.count == 0 {
                        break;
                    }
                    for _ in 0..block.count {
                        child.decode(dec, schema)?;
                    }
                    length += block.count as u64;
                }
                entries.push(duckdb_v2_list_entry { offset, length });
            }
            Node::Map { entries, key, value, from_array, .. } => {
                let offset = key.len() as u64;
                let mut length = 0u64;
                loop {
                    let block = dec.read_block_count().or_err()?;
                    if block.count == 0 {
                        break;
                    }
                    for _ in 0..block.count {
                        if *from_array {
                            key.decode(dec, schema)?;
                        } else {
                            match key.as_mut() {
                                Node::String(s) => s.push(dec.read_string().or_err()?.as_bytes()),
                                _ => return Err(Error::invalid_input("Avro map keys must be strings")),
                            }
                        }
                        value.decode(dec, schema)?;
                    }
                    length += block.count as u64;
                }
                entries.push(duckdb_v2_list_entry { offset, length });
            }
            Node::Union { branch_ids, branch_members, members, tags, nulls } => {
                let branch = dec.read_union_index(branch_ids.len()).or_err()?;
                match branch_members[branch] {
                    None => {
                        dec.skip(schema, branch_ids[branch]).or_err()?;
                        nulls.push(tags.len());
                        tags.push(0);
                        members.iter_mut().for_each(Node::push_null);
                    }
                    Some(member) => {
                        tags.push(member as u8);
                        for (index, candidate) in members.iter_mut().enumerate() {
                            if index == member {
                                candidate.decode(dec, schema)?;
                            } else {
                                candidate.push_null();
                            }
                        }
                    }
                }
            }
            Node::Nullable { branch_ids, branch, child } => {
                let selected = dec.read_union_index(branch_ids.len()).or_err()?;
                if selected == *branch {
                    child.decode(dec, schema)?;
                } else {
                    dec.skip(schema, branch_ids[selected]).or_err()?;
                    child.push_null();
                }
            }
        }
        Ok(())
    }

    /// Writes the buffered values into `vector` (sized to them) and its children.
    pub(crate) fn flush(&self, vector: &Vector) -> Result<()> {
        match self {
            Node::Null { len, .. } => {
                if *len > 0 {
                    vector.flat_validity_mut(*len)?.set_all_invalid();
                }
                Ok(())
            }
            Node::Boolean(p) => p.flush(vector),
            Node::Int(p) => p.flush(vector),
            Node::Long(p) | Node::TimeMillis(p) | Node::TimestampMillis(p) => p.flush(vector),
            Node::Float(p) => p.flush(vector),
            Node::Double(p) => p.flush(vector),
            Node::Bytes(s) | Node::String(s) => s.flush(vector),
            Node::Fixed { values, .. } => values.flush(vector),
            Node::Uuid(p) => p.flush(vector),
            Node::Decimal { values, .. } => match values {
                DecimalValues::I16(p) => p.flush(vector),
                DecimalValues::I32(p) => p.flush(vector),
                DecimalValues::I64(p) => p.flush(vector),
                DecimalValues::I128(p) => p.flush(vector),
            },
            Node::Enum { values, .. } => match values {
                EnumValues::U8(p) => p.flush(vector),
                EnumValues::U16(p) => p.flush(vector),
                EnumValues::U32(p) => p.flush(vector),
            },
            Node::Struct { fields, len, nulls } => {
                if vector.size()? < *len {
                    vector.set_size(*len)?;
                }
                for (index, field) in fields.iter().enumerate() {
                    field.flush(&vector.child(index)?)?;
                }
                set_nulls(vector, *len, nulls)
            }
            Node::List { entries, child, nulls } => {
                let count = entries.len();
                vector.flat_data_mut::<duckdb_v2_list_entry>(count)?.copy_from_slice(entries);
                set_nulls(vector, count, nulls)?;
                let elements = vector.child(0)?;
                elements.set_size(child.len())?;
                child.flush(&elements)
            }
            Node::Map { entries, key, value, nulls, .. } => {
                let count = entries.len();
                vector.flat_data_mut::<duckdb_v2_list_entry>(count)?.copy_from_slice(entries);
                set_nulls(vector, count, nulls)?;
                let key_value = vector.child(0)?;
                key_value.set_size(key.len())?;
                key.flush(&key_value.child(0)?)?;
                value.flush(&key_value.child(1)?)
            }
            Node::Union { members, tags, nulls, .. } => {
                let count = tags.len();
                if vector.size()? < count {
                    vector.set_size(count)?;
                }
                let tag_vector = vector.child(0)?;
                tag_vector.flat_data_mut::<u8>(count)?.copy_from_slice(tags);
                set_nulls(&tag_vector, count, nulls)?;
                for (index, member) in members.iter().enumerate() {
                    member.flush(&vector.child(index + 1)?)?;
                }
                set_nulls(vector, count, nulls)
            }
            Node::Nullable { child, .. } => child.flush(vector),
        }
    }
}

/// A big-endian two's complement integer of any length as an `i128` (sign-extended; bytes beyond 16 shift out, as the
/// C++ reader's shifts do).
fn read_decimal(raw: &[u8]) -> i128 {
    let negative = raw.first().is_some_and(|b| b & 0x80 != 0);
    let mut value: i128 = if negative { -1 } else { 0 };
    for &byte in raw {
        value = (value << 8) | i128::from(byte);
    }
    value
}

/// Maps the result of an `avro-core` call onto the engine's error classes.
trait AvroResultExt<T> {
    fn or_err(self) -> Result<T>;
}

impl<T> AvroResultExt<T> for avro_core::Result<T> {
    #[inline]
    fn or_err(self) -> Result<T> {
        self.map_err(super::file::convert_error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decimals_sign_extend() {
        assert_eq!(read_decimal(&[]), 0);
        assert_eq!(read_decimal(&[0x00]), 0);
        assert_eq!(read_decimal(&[0x7f]), 127);
        assert_eq!(read_decimal(&[0x80]), -128);
        assert_eq!(read_decimal(&[0xff]), -1);
        assert_eq!(read_decimal(&[0x00, 0x80]), 128);
        assert_eq!(read_decimal(&[0xff, 0x7f]), -129);
        assert_eq!(read_decimal(&[0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]), 1i128 << 64);
    }
}
