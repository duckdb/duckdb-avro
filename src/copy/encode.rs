//! Encoding the rows of a chunk as Avro binary, column by column, with the nullability the schema gives each field.
//!
//! A [`ColumnPlan`] mirrors the Avro schema of a column (built once, from the DuckDB type and the FIELD_IDS); a
//! [`Prepared`] is the plan with the read views of one chunk's vectors (the C++ `Prepare`); [`Prepared::write`]
//! encodes one row. The row layout is the schema's: a nullable field is a union (`0` for null, `1` plus the value),
//! a record its fields back to back, an array one block of `n` items plus the end marker, a MAP an array of
//! key/value records, an ENUM the symbol index, a UNION the branch index (the member tag, plus one when the field is
//! nullable and `null` is the first branch) followed by the member's value.

use avro_core::Encoder;
use duckdb_ext_api::sys::{DUCKDB_V2_VECTOR_TYPE_DICTIONARY, duckdb_v2_hugeint_t};
use duckdb_ext_api::types::type_id;
use duckdb_ext_api::{Error, ListView, LogicalType, Result, StringView, TypedView, Vector, VectorView};

use super::field_ids::FieldId;
use super::schema::{EMPTY_STRUCT_MARKER, min_bytes_required_for_decimal};
use super::types::{
    decimal_internal_type_id, decimal_width_scale, list_child_type, map_key_value_types, struct_children, union_members,
};

/// How the elements of one column (or nested child) are encoded.
#[derive(Debug, Clone)]
pub(crate) enum Plan {
    /// The `null` type: nothing is written.
    Null,
    Boolean,
    /// INTEGER and DATE: an Avro `int`.
    Int32,
    /// BIGINT, TIME and every TIMESTAMP flavour: an Avro `long` of the stored value.
    Int64,
    Float,
    Double,
    String,
    Bytes,
    /// A `fixed(16)` of the UUID bytes.
    Uuid,
    /// A `fixed` of the two's complement of the unscaled value, sized for the width.
    Decimal {
        width: u8,
        internal: DecimalInternal,
    },
    /// A record: the vector children (by index, the empty struct marker skipped) with their plans.
    Struct {
        children: Vec<(usize, ColumnPlan)>,
    },
    /// An array (LIST, or MAP over its key/value entries).
    List {
        child: Box<ColumnPlan>,
    },
    /// An `enum`: the symbol index, stored in the width the symbol count needs.
    Enum(EnumWidth),
    /// A `union`: one plan per member (none of them nullable: a member has no null slot of its own).
    Union {
        members: Vec<ColumnPlan>,
    },
}

/// The storage type of an ENUM's elements.
#[derive(Debug, Clone, Copy)]
pub(crate) enum EnumWidth {
    U8,
    U16,
    U32,
}

/// The storage type of a DECIMAL's elements.
#[derive(Debug, Clone, Copy)]
pub(crate) enum DecimalInternal {
    Int16,
    Int32,
    Int64,
    Int128,
}

/// The plan of one field of the schema: its encoding, whether its type is wrapped in a union with `null`, and the
/// DuckDB type name for the error a NULL in a non-nullable field raises.
#[derive(Debug, Clone)]
pub(crate) struct ColumnPlan {
    plan: Plan,
    nullable: bool,
    type_name: String,
}

impl ColumnPlan {
    /// The plan of a column of `type_` with the FIELD_IDS entry `field_id` (the nullability source).
    pub(crate) fn new(type_: &LogicalType, field_id: Option<&FieldId>) -> Result<ColumnPlan> {
        let nullable = field_id.is_none_or(|id| id.nullable);
        let type_name = type_.name()?;
        let plan = match type_.id()? {
            type_id::BOOLEAN => Plan::Boolean,
            type_id::BLOB => Plan::Bytes,
            type_id::DOUBLE => Plan::Double,
            type_id::FLOAT => Plan::Float,
            type_id::INTEGER | type_id::DATE => Plan::Int32,
            type_id::BIGINT
            | type_id::TIME
            | type_id::TIMESTAMP
            | type_id::TIMESTAMP_MS
            | type_id::TIMESTAMP_NS
            | type_id::TIMESTAMP_TZ
            | type_id::TIMESTAMP_TZ_NS => Plan::Int64,
            type_id::VARCHAR => Plan::String,
            type_id::UUID => Plan::Uuid,
            type_id::DECIMAL => {
                let (width, _) = decimal_width_scale(type_)?;
                let internal = match decimal_internal_type_id(width) {
                    type_id::SMALLINT => DecimalInternal::Int16,
                    type_id::INTEGER => DecimalInternal::Int32,
                    type_id::BIGINT => DecimalInternal::Int64,
                    type_id::HUGEINT => DecimalInternal::Int128,
                    _ => return Err(Error::not_implemented("Unsupported decimal physical type")),
                };
                Plan::Decimal { width, internal }
            }
            type_id::SQLNULL => Plan::Null,
            type_id::STRUCT => {
                let mut children = Vec::new();
                for (index, (child_name, child_type)) in struct_children(type_)?.into_iter().enumerate() {
                    if child_name == EMPTY_STRUCT_MARKER {
                        continue;
                    }
                    let child_id = field_id.and_then(|id| id.children.find(&child_name));
                    children.push((index, ColumnPlan::new(&child_type, child_id)?));
                }
                Plan::Struct { children }
            }
            type_id::MAP => {
                // the entries of a MAP are written as records of their key and value
                let (key_type, value_type) = map_key_value_types(type_)?;
                let key = ColumnPlan::new(&key_type, field_id.and_then(|id| id.children.find("key")))?;
                let value = ColumnPlan::new(&value_type, field_id.and_then(|id| id.children.find("value")))?;
                let entries = ColumnPlan {
                    plan: Plan::Struct { children: vec![(0, key), (1, value)] },
                    // the key/value record itself is never wrapped in a union
                    nullable: false,
                    type_name: "STRUCT".to_owned(),
                };
                Plan::List { child: Box::new(entries) }
            }
            type_id::LIST => {
                let child_type = list_child_type(type_)?;
                let child = ColumnPlan::new(&child_type, field_id.and_then(|id| id.children.find("list")))?;
                Plan::List { child: Box::new(child) }
            }
            type_id::ENUM => Plan::Enum(match type_.element_width()? {
                Some(1) => EnumWidth::U8,
                Some(2) => EnumWidth::U16,
                _ => EnumWidth::U32,
            }),
            type_id::UNION => {
                let mut members = Vec::new();
                for (member_name, member_type) in union_members(type_)? {
                    let mut member =
                        ColumnPlan::new(&member_type, field_id.and_then(|id| id.children.find(&member_name)))?;
                    // the union's null branch is the only null slot
                    member.nullable = false;
                    members.push(member);
                }
                Plan::Union { members }
            }
            _ => {
                return Err(Error::not_implemented(format!(
                    "PopulateValue not implemented for type {}",
                    type_.to_text()?
                )));
            }
        };
        Ok(ColumnPlan { plan, nullable, type_name })
    }

    /// The plan over the vector of one chunk (the C++ `Prepare`): a DICTIONARY STRUCT or LIST vector is flattened
    /// first, so that its children are addressed by row.
    pub(crate) fn prepare(&self, vector: &Vector) -> Result<Prepared<'_>> {
        let view = match &self.plan {
            Plan::Null => View::Null,
            Plan::Boolean => View::Boolean(vector.typed_view::<u8>()?),
            Plan::Int32 => View::Int32(vector.typed_view::<i32>()?),
            Plan::Int64 => View::Int64(vector.typed_view::<i64>()?),
            Plan::Float => View::Float(vector.typed_view::<f32>()?),
            Plan::Double => View::Double(vector.typed_view::<f64>()?),
            Plan::String => View::String(vector.string_view()?),
            Plan::Bytes => View::Bytes(vector.string_view()?),
            Plan::Uuid => View::Uuid(vector.typed_view::<duckdb_v2_hugeint_t>()?),
            Plan::Decimal { width, internal } => {
                let width = *width;
                match internal {
                    DecimalInternal::Int16 => View::Decimal16(vector.typed_view::<i16>()?, width),
                    DecimalInternal::Int32 => View::Decimal32(vector.typed_view::<i32>()?, width),
                    DecimalInternal::Int64 => View::Decimal64(vector.typed_view::<i64>()?, width),
                    DecimalInternal::Int128 => View::Decimal128(vector.typed_view::<duckdb_v2_hugeint_t>()?, width),
                }
            }
            Plan::Struct { children } => {
                if vector.vector_type()? == DUCKDB_V2_VECTOR_TYPE_DICTIONARY {
                    vector.flatten()?;
                }
                let view = vector.view()?;
                let mut prepared = Vec::with_capacity(children.len());
                for (index, child) in children {
                    prepared.push(child.prepare(&vector.child(*index)?)?);
                }
                View::Struct(view, prepared)
            }
            Plan::List { child } => {
                if vector.vector_type()? == DUCKDB_V2_VECTOR_TYPE_DICTIONARY {
                    vector.flatten()?;
                }
                let view = vector.list_view()?;
                View::List(view, Box::new(child.prepare(&vector.child(0)?)?))
            }
            Plan::Enum(width) => match width {
                EnumWidth::U8 => View::Enum8(vector.typed_view::<u8>()?),
                EnumWidth::U16 => View::Enum16(vector.typed_view::<u16>()?),
                EnumWidth::U32 => View::Enum32(vector.typed_view::<u32>()?),
            },
            Plan::Union { members } => {
                // a UNION vector is a STRUCT vector of the tag (UTINYINT) and the members
                if vector.vector_type()? == DUCKDB_V2_VECTOR_TYPE_DICTIONARY {
                    vector.flatten()?;
                }
                let view = vector.view()?;
                let tags = vector.child(0)?.typed_view::<u8>()?;
                let mut prepared = Vec::with_capacity(members.len());
                for (index, member) in members.iter().enumerate() {
                    prepared.push(member.prepare(&vector.child(index + 1)?)?);
                }
                View::Union(view, tags, prepared)
            }
        };
        Ok(Prepared { plan: self, view })
    }
}

/// The read views of one chunk's vector for a plan.
enum View<'p> {
    Null,
    Boolean(TypedView<u8>),
    Int32(TypedView<i32>),
    Int64(TypedView<i64>),
    Float(TypedView<f32>),
    Double(TypedView<f64>),
    String(StringView),
    Bytes(StringView),
    Uuid(TypedView<duckdb_v2_hugeint_t>),
    Decimal16(TypedView<i16>, u8),
    Decimal32(TypedView<i32>, u8),
    Decimal64(TypedView<i64>, u8),
    Decimal128(TypedView<duckdb_v2_hugeint_t>, u8),
    Struct(VectorView, Vec<Prepared<'p>>),
    List(ListView, Box<Prepared<'p>>),
    Enum8(TypedView<u8>),
    Enum16(TypedView<u16>),
    Enum32(TypedView<u32>),
    /// The union's own validity, its tags, and the members.
    Union(VectorView, TypedView<u8>, Vec<Prepared<'p>>),
}

/// A plan with the views of one chunk's vector; valid as long as the chunk.
pub(crate) struct Prepared<'p> {
    plan: &'p ColumnPlan,
    view: View<'p>,
}

impl Prepared<'_> {
    /// Encodes row `row` of the vector.
    pub(crate) fn write(&self, enc: &mut Encoder, row: usize) -> Result<()> {
        match &self.view {
            View::Null => return self.write_null(enc),
            View::Boolean(v) => match v.get_valid(row) {
                Some(b) => self.non_null(enc).write_boolean(b != 0),
                None => return self.write_null(enc),
            },
            View::Int32(v) => match v.get_valid(row) {
                Some(i) => self.non_null(enc).write_int(i),
                None => return self.write_null(enc),
            },
            View::Int64(v) => match v.get_valid(row) {
                Some(i) => self.non_null(enc).write_long(i),
                None => return self.write_null(enc),
            },
            View::Float(v) => match v.get_valid(row) {
                Some(f) => self.non_null(enc).write_float(f),
                None => return self.write_null(enc),
            },
            View::Double(v) => match v.get_valid(row) {
                Some(f) => self.non_null(enc).write_double(f),
                None => return self.write_null(enc),
            },
            View::String(v) => match v.valid_bytes(row) {
                Some(bytes) => self.non_null(enc).write_bytes(bytes),
                None => return self.write_null(enc),
            },
            View::Bytes(v) => match v.valid_bytes(row) {
                Some(bytes) => self.non_null(enc).write_bytes(bytes),
                None => return self.write_null(enc),
            },
            View::Uuid(v) => match v.get_valid(row) {
                Some(h) => self.non_null(enc).write_fixed(&uuid_bytes(h)),
                None => return self.write_null(enc),
            },
            View::Decimal16(v, width) => match v.get_valid(row) {
                Some(d) => write_decimal(self.non_null(enc), i128::from(d), *width),
                None => return self.write_null(enc),
            },
            View::Decimal32(v, width) => match v.get_valid(row) {
                Some(d) => write_decimal(self.non_null(enc), i128::from(d), *width),
                None => return self.write_null(enc),
            },
            View::Decimal64(v, width) => match v.get_valid(row) {
                Some(d) => write_decimal(self.non_null(enc), i128::from(d), *width),
                None => return self.write_null(enc),
            },
            View::Decimal128(v, width) => match v.get_valid(row) {
                Some(h) => write_decimal(self.non_null(enc), hugeint_to_i128(h), *width),
                None => return self.write_null(enc),
            },
            View::Struct(view, children) => {
                if !view.is_valid(row) {
                    return self.write_null(enc);
                }
                self.non_null(enc);
                for child in children {
                    child.write(enc, row)?;
                }
            }
            View::List(view, child) => {
                let Some(entry) = view.get_valid(row) else {
                    return self.write_null(enc);
                };
                self.non_null(enc);
                let (offset, length) = (entry.offset as usize, entry.length as usize);
                if length > 0 {
                    enc.write_block_count(length);
                    for i in offset..offset + length {
                        child.write(enc, i)?;
                    }
                }
                enc.write_block_end();
            }
            View::Enum8(v) => match v.get_valid(row) {
                Some(i) => self.non_null(enc).write_enum(usize::from(i)),
                None => return self.write_null(enc),
            },
            View::Enum16(v) => match v.get_valid(row) {
                Some(i) => self.non_null(enc).write_enum(usize::from(i)),
                None => return self.write_null(enc),
            },
            View::Enum32(v) => match v.get_valid(row) {
                Some(i) => self.non_null(enc).write_enum(i as usize),
                None => return self.write_null(enc),
            },
            View::Union(view, tags, members) => {
                let Some(tag) = tags.get_valid(row).filter(|_| view.is_valid(row)) else {
                    return self.write_null(enc);
                };
                let member = members.get(usize::from(tag)).ok_or_else(|| {
                    Error::invalid_input(format!("UNION tag {tag} beyond the {} members", members.len()))
                })?;
                // the branch index: the tag, after the null branch of a nullable field
                enc.write_union_index(usize::from(tag) + usize::from(self.plan.nullable));
                member.write(enc, row)?;
            }
        }
        Ok(())
    }

    /// A NULL: the null branch of the union, or an error for a field whose type is not nullable (the `null` type
    /// takes it as it is).
    fn write_null(&self, enc: &mut Encoder) -> Result<()> {
        if matches!(self.plan.plan, Plan::Null) {
            enc.write_null();
            return Ok(());
        }
        if !self.plan.nullable {
            return Err(Error::invalid_input(format!(
                "Cannot insert NULL to non-nullable field of type {}",
                self.plan.type_name
            )));
        }
        enc.write_union_index(0);
        Ok(())
    }

    /// Selects the non-null branch of a nullable field; returns the encoder for the value.
    #[inline]
    fn non_null<'e>(&self, enc: &'e mut Encoder) -> &'e mut Encoder {
        if self.plan.nullable {
            enc.write_union_index(1);
        }
        enc
    }
}

fn hugeint_to_i128(h: duckdb_v2_hugeint_t) -> i128 {
    ((h.upper as i128) << 64) | i128::from(h.lower)
}

/// The 16 bytes of a UUID from the engine's storage form: the high bit of the upper half is flipped (so that the
/// integer orders like the UUID), both halves big-endian.
fn uuid_bytes(h: duckdb_v2_hugeint_t) -> [u8; 16] {
    let upper = (h.upper as u64) ^ (1u64 << 63);
    let mut bytes = [0u8; 16];
    bytes[..8].copy_from_slice(&upper.to_be_bytes());
    bytes[8..].copy_from_slice(&h.lower.to_be_bytes());
    bytes
}

/// A DECIMAL as the big-endian two's complement bytes its precision requires.
fn write_decimal(enc: &mut Encoder, unscaled: i128, width: u8) {
    let full = unscaled.to_be_bytes();
    let bytes_needed = min_bytes_required_for_decimal(width);
    enc.write_fixed(&full[full.len() - bytes_needed..]);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decimal_bytes() {
        let mut enc = Encoder::new();
        write_decimal(&mut enc, -1, 1);
        write_decimal(&mut enc, 99, 2);
        write_decimal(&mut enc, -99, 2);
        write_decimal(&mut enc, 1000, 4);
        write_decimal(&mut enc, i128::from(i64::MIN), 19);
        assert_eq!(enc.as_bytes(), [0xff, 99, 0x9d, 0x03, 0xe8, 0xff, 0x80, 0, 0, 0, 0, 0, 0, 0].as_slice());
    }

    #[test]
    fn uuid_layout() {
        // 00000000-0000-0000-0000-000000000001 is stored with the high bit of `upper` flipped
        let h = duckdb_v2_hugeint_t { lower: 1, upper: i64::MIN };
        let mut expected = [0u8; 16];
        expected[15] = 1;
        assert_eq!(uuid_bytes(h), expected);
        let h = duckdb_v2_hugeint_t { lower: 0x8899aabb_ccddeeff, upper: 0x00112233_44556677_u64 as i64 ^ i64::MIN };
        assert_eq!(
            uuid_bytes(h),
            [0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff]
        );
    }
}
