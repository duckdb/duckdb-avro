//! Introspection of the DuckDB types the writer converts: the children of STRUCT / LIST / MAP and the width and scale
//! of a DECIMAL, read from the type's value parameters (`duckdb_v2_logical_type_get_param`: a STRUCT has one TYPE
//! parameter per field under the field's name, a LIST its element type, a MAP its key and value types, a DECIMAL its
//! width and scale as UTINYINT).

use duckdb_ext_api::types::type_id;
use duckdb_ext_api::{Error, LogicalType, Result, TypeId};

/// The `(name, type)` of every field of a STRUCT.
pub(crate) fn struct_children(type_: &LogicalType) -> Result<Vec<(String, LogicalType)>> {
    let count = type_.param_count()?;
    let mut children = Vec::with_capacity(count);
    for i in 0..count {
        let (name, value) = type_.param(i)?;
        children.push((name, value.as_type()?));
    }
    Ok(children)
}

/// The element type of a LIST.
pub(crate) fn list_child_type(type_: &LogicalType) -> Result<LogicalType> {
    type_.param(0)?.1.as_type()
}

/// The `(key, value)` types of a MAP.
pub(crate) fn map_key_value_types(type_: &LogicalType) -> Result<(LogicalType, LogicalType)> {
    Ok((type_.param(0)?.1.as_type()?, type_.param(1)?.1.as_type()?))
}

/// The `(width, scale)` of a DECIMAL.
pub(crate) fn decimal_width_scale(type_: &LogicalType) -> Result<(u8, u8)> {
    Ok((type_.param(0)?.1.as_u8()?, type_.param(1)?.1.as_u8()?))
}

/// The storage type of a DECIMAL's vector elements by its width (`duckdb_v2.h`: "A DECIMAL of width <= 4 stores int16,
/// <= 9 int32, <= 18 int64, and <= 38 int128").
pub(crate) fn decimal_internal_type_id(width: u8) -> TypeId {
    match width {
        0..=4 => type_id::SMALLINT,
        5..=9 => type_id::INTEGER,
        10..=18 => type_id::BIGINT,
        _ => type_id::HUGEINT,
    }
}

/// The error the C++ reports for a type the writer has no Avro type for.
pub(crate) fn cannot_convert(type_: &LogicalType) -> Error {
    match type_.to_text() {
        Ok(text) => Error::not_implemented(format!("Can't convert logical type '{text}' to Avro type")),
        Err(error) => error,
    }
}
