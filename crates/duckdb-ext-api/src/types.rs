//! Logical types (`duckdb_v2_logical_type_handle`, owned).

use std::fmt;

use crate::Result;
use crate::context::Factory;
use crate::qname::QualifiedName;
use crate::sys::*;
use crate::text::{render, str_arg, str_to_string};
use crate::value::Value;

/// `DUCKDB_V2_LOGICAL_TYPE_ID`: the members are the `DUCKDB_V2_LOGICAL_TYPE_ID_*` constants of
/// [`crate::sys`] (`TypeId::INTEGER` etc. through the associated constants below).
pub type TypeId = DUCKDB_V2_LOGICAL_TYPE_ID;

/// Readable names for the type ids.
pub mod type_id {
    use crate::sys::*;
    pub const INVALID: DUCKDB_V2_LOGICAL_TYPE_ID = DUCKDB_V2_LOGICAL_TYPE_ID_INVALID;
    pub const SQLNULL: DUCKDB_V2_LOGICAL_TYPE_ID = DUCKDB_V2_LOGICAL_TYPE_ID_SQLNULL;
    pub const ANY: DUCKDB_V2_LOGICAL_TYPE_ID = DUCKDB_V2_LOGICAL_TYPE_ID_ANY;
    pub const BOOLEAN: DUCKDB_V2_LOGICAL_TYPE_ID = DUCKDB_V2_LOGICAL_TYPE_ID_BOOLEAN;
    pub const TINYINT: DUCKDB_V2_LOGICAL_TYPE_ID = DUCKDB_V2_LOGICAL_TYPE_ID_TINYINT;
    pub const SMALLINT: DUCKDB_V2_LOGICAL_TYPE_ID = DUCKDB_V2_LOGICAL_TYPE_ID_SMALLINT;
    pub const INTEGER: DUCKDB_V2_LOGICAL_TYPE_ID = DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER;
    pub const BIGINT: DUCKDB_V2_LOGICAL_TYPE_ID = DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT;
    pub const UTINYINT: DUCKDB_V2_LOGICAL_TYPE_ID = DUCKDB_V2_LOGICAL_TYPE_ID_UTINYINT;
    pub const USMALLINT: DUCKDB_V2_LOGICAL_TYPE_ID = DUCKDB_V2_LOGICAL_TYPE_ID_USMALLINT;
    pub const UINTEGER: DUCKDB_V2_LOGICAL_TYPE_ID = DUCKDB_V2_LOGICAL_TYPE_ID_UINTEGER;
    pub const UBIGINT: DUCKDB_V2_LOGICAL_TYPE_ID = DUCKDB_V2_LOGICAL_TYPE_ID_UBIGINT;
    pub const HUGEINT: DUCKDB_V2_LOGICAL_TYPE_ID = DUCKDB_V2_LOGICAL_TYPE_ID_HUGEINT;
    pub const UHUGEINT: DUCKDB_V2_LOGICAL_TYPE_ID = DUCKDB_V2_LOGICAL_TYPE_ID_UHUGEINT;
    pub const FLOAT: DUCKDB_V2_LOGICAL_TYPE_ID = DUCKDB_V2_LOGICAL_TYPE_ID_FLOAT;
    pub const DOUBLE: DUCKDB_V2_LOGICAL_TYPE_ID = DUCKDB_V2_LOGICAL_TYPE_ID_DOUBLE;
    pub const DECIMAL: DUCKDB_V2_LOGICAL_TYPE_ID = DUCKDB_V2_LOGICAL_TYPE_ID_DECIMAL;
    pub const VARCHAR: DUCKDB_V2_LOGICAL_TYPE_ID = DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR;
    pub const BLOB: DUCKDB_V2_LOGICAL_TYPE_ID = DUCKDB_V2_LOGICAL_TYPE_ID_BLOB;
    pub const BIT: DUCKDB_V2_LOGICAL_TYPE_ID = DUCKDB_V2_LOGICAL_TYPE_ID_BIT;
    pub const DATE: DUCKDB_V2_LOGICAL_TYPE_ID = DUCKDB_V2_LOGICAL_TYPE_ID_DATE;
    pub const TIME: DUCKDB_V2_LOGICAL_TYPE_ID = DUCKDB_V2_LOGICAL_TYPE_ID_TIME;
    pub const TIMESTAMP: DUCKDB_V2_LOGICAL_TYPE_ID = DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP;
    pub const TIMESTAMP_TZ: DUCKDB_V2_LOGICAL_TYPE_ID = DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_TZ;
    pub const TIMESTAMP_TZ_NS: DUCKDB_V2_LOGICAL_TYPE_ID = DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_TZ_NS;
    pub const TIMESTAMP_SEC: DUCKDB_V2_LOGICAL_TYPE_ID = DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_SEC;
    pub const TIMESTAMP_MS: DUCKDB_V2_LOGICAL_TYPE_ID = DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_MS;
    pub const TIMESTAMP_NS: DUCKDB_V2_LOGICAL_TYPE_ID = DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_NS;
    pub const TIME_TZ: DUCKDB_V2_LOGICAL_TYPE_ID = DUCKDB_V2_LOGICAL_TYPE_ID_TIME_TZ;
    pub const TIME_NS: DUCKDB_V2_LOGICAL_TYPE_ID = DUCKDB_V2_LOGICAL_TYPE_ID_TIME_NS;
    pub const INTERVAL: DUCKDB_V2_LOGICAL_TYPE_ID = DUCKDB_V2_LOGICAL_TYPE_ID_INTERVAL;
    pub const UUID: DUCKDB_V2_LOGICAL_TYPE_ID = DUCKDB_V2_LOGICAL_TYPE_ID_UUID;
    pub const STRUCT: DUCKDB_V2_LOGICAL_TYPE_ID = DUCKDB_V2_LOGICAL_TYPE_ID_STRUCT;
    pub const LIST: DUCKDB_V2_LOGICAL_TYPE_ID = DUCKDB_V2_LOGICAL_TYPE_ID_LIST;
    pub const MAP: DUCKDB_V2_LOGICAL_TYPE_ID = DUCKDB_V2_LOGICAL_TYPE_ID_MAP;
    pub const ARRAY: DUCKDB_V2_LOGICAL_TYPE_ID = DUCKDB_V2_LOGICAL_TYPE_ID_ARRAY;
    pub const UNION: DUCKDB_V2_LOGICAL_TYPE_ID = DUCKDB_V2_LOGICAL_TYPE_ID_UNION;
    pub const ENUM: DUCKDB_V2_LOGICAL_TYPE_ID = DUCKDB_V2_LOGICAL_TYPE_ID_ENUM;
    pub const TYPE: DUCKDB_V2_LOGICAL_TYPE_ID = DUCKDB_V2_LOGICAL_TYPE_ID_TYPE;
}

/// An owned logical type.
pub struct LogicalType {
    raw: duckdb_v2_logical_type_handle,
}

// SAFETY(protocol L8.I8): (T5, L8.I1) an owned `duckdb_v2_logical_type_handle` (`duckdb_v2.h`: "A type is read-only
// once constructed"); every entry the wrapper calls after construction reads the type (`get_id`, `get_name`,
// `get_param*`, `is_equal`, `to_text`, `copy`), so a shared `&LogicalType` is a shared read and the owned type moves
// between threads as a whole.
unsafe impl Send for LogicalType {}
unsafe impl Sync for LogicalType {}

impl LogicalType {
    /// Takes ownership of a handle the API returned.
    /// # Safety
    /// (ffi L8.I1) `raw` must be a handle the C API returned for this type and that is still live. An owned wrapper
    /// destroys the handle on drop, so nothing else may destroy it; a borrowed (`Copy`) wrapper must not be used past
    /// the call that lent the handle.
    pub unsafe fn from_raw(raw: duckdb_v2_logical_type_handle) -> LogicalType {
        LogicalType { raw }
    }

    pub fn as_raw(&self) -> duckdb_v2_logical_type_handle {
        self.raw
    }

    /// Gives up ownership (for an API call that consumes the handle).
    pub fn into_raw(self) -> duckdb_v2_logical_type_handle {
        let raw = self.raw;
        std::mem::forget(self);
        raw
    }

    /// A type without parameters (`duckdb_v2_factory_create_type_from_id`).
    pub fn new(factory: Factory, id: TypeId) -> Result<LogicalType> {
        LogicalType::with_params(factory, id, &[])
    }

    /// A parameterised type from its id, e.g. `DECIMAL(width, scale)`, `LIST(child)`, `STRUCT(a INTEGER, ...)`:
    /// the parameters are `(name, value)` pairs in the order the type takes them (the name is empty for
    /// positional parameters; TYPE-valued parameters are [`Value::type_value`]).
    pub fn with_params(factory: Factory, id: TypeId, params: &[(&str, &Value)]) -> Result<LogicalType> {
        let names: Vec<duckdb_v2_identifier_t> = params.iter().map(|(name, _)| param_name(name)).collect();
        let values: Vec<duckdb_v2_value_handle> = params.iter().map(|(_, value)| value.as_raw()).collect();
        let mut raw: duckdb_v2_logical_type_handle = std::ptr::null_mut();
        api_call!(duckdb_v2_factory_create_type_from_id(
            factory.as_raw(),
            id,
            if names.is_empty() { std::ptr::null() } else { names.as_ptr() },
            if values.is_empty() { std::ptr::null() } else { values.as_ptr() },
            params.len() as idx_t,
            &mut raw
        ));
        Ok(LogicalType { raw })
    }

    /// Parses SQL type text, e.g. `"STRUCT(a INTEGER, b VARCHAR)"` (`duckdb_v2_factory_create_type_from_text`;
    /// resolves catalog types, so it needs a factory with a context when the type is user-defined).
    pub fn parse(factory: Factory, text: &str) -> Result<LogicalType> {
        let text = str_arg(text);
        let mut raw: duckdb_v2_logical_type_handle = std::ptr::null_mut();
        api_call!(duckdb_v2_factory_create_type_from_text(factory.as_raw(), &text, &mut raw));
        Ok(LogicalType { raw })
    }

    /// A catalog type by (qualified) name, with parameters for its constructors
    /// (`duckdb_v2_factory_create_type_from_name`).
    pub fn from_name(factory: Factory, name: &QualifiedName, params: &[(&str, &Value)]) -> Result<LogicalType> {
        let names: Vec<duckdb_v2_identifier_t> = params.iter().map(|(name, _)| param_name(name)).collect();
        let values: Vec<duckdb_v2_value_handle> = params.iter().map(|(_, value)| value.as_raw()).collect();
        let mut raw: duckdb_v2_logical_type_handle = std::ptr::null_mut();
        api_call!(duckdb_v2_factory_create_type_from_name(
            factory.as_raw(),
            name.as_raw(),
            if names.is_empty() { std::ptr::null() } else { names.as_ptr() },
            if values.is_empty() { std::ptr::null() } else { values.as_ptr() },
            params.len() as idx_t,
            &mut raw
        ));
        Ok(LogicalType { raw })
    }

    /// The same type under an alias (`duckdb_v2_factory_create_type_with_alias`).
    pub fn with_alias(&self, factory: Factory, alias: &str) -> Result<LogicalType> {
        let alias = str_arg(alias);
        let mut raw: duckdb_v2_logical_type_handle = std::ptr::null_mut();
        api_call!(duckdb_v2_factory_create_type_with_alias(factory.as_raw(), self.raw, &alias, &mut raw));
        Ok(LogicalType { raw })
    }

    /// The type of the unshredded component of a shredded VARIANT vector: a STRUCT of the canonical VARIANT
    /// layout's children (`duckdb_v2_factory_create_variant_unshredded_type`).
    pub fn variant_unshredded(factory: Factory) -> Result<LogicalType> {
        let mut raw: duckdb_v2_logical_type_handle = std::ptr::null_mut();
        api_call!(duckdb_v2_factory_create_variant_unshredded_type(factory.as_raw(), &mut raw));
        Ok(LogicalType { raw })
    }

    /// `LIST(child)`.
    pub fn list(factory: Factory, child: &LogicalType) -> Result<LogicalType> {
        let child = Value::type_value(factory, child)?;
        LogicalType::with_params(factory, type_id::LIST, &[("", &child)])
    }

    /// `STRUCT(name type, ...)`.
    pub fn struct_(factory: Factory, fields: &[(&str, &LogicalType)]) -> Result<LogicalType> {
        let values: Vec<Value> =
            fields.iter().map(|(_, t)| Value::type_value(factory, t)).collect::<Result<Vec<Value>>>()?;
        let params: Vec<(&str, &Value)> = fields.iter().zip(values.iter()).map(|((n, _), v)| (*n, v)).collect();
        LogicalType::with_params(factory, type_id::STRUCT, &params)
    }

    /// `DECIMAL(width, scale)`.
    pub fn decimal(factory: Factory, width: u8, scale: u8) -> Result<LogicalType> {
        let width = Value::utinyint(factory, width)?;
        let scale = Value::utinyint(factory, scale)?;
        LogicalType::with_params(factory, type_id::DECIMAL, &[("", &width), ("", &scale)])
    }

    /// The common super type of `types` (`duckdb_v2_factory_create_common_type`).
    pub fn common_type(factory: Factory, types: &[&LogicalType]) -> Result<LogicalType> {
        let raws: Vec<duckdb_v2_logical_type_handle> = types.iter().map(|t| t.raw).collect();
        let mut raw: duckdb_v2_logical_type_handle = std::ptr::null_mut();
        api_call!(duckdb_v2_factory_create_common_type(factory.as_raw(), raws.as_ptr(), raws.len() as idx_t, &mut raw));
        Ok(LogicalType { raw })
    }

    /// The incomplete template of a family of parameterized types (`LogicalType::LIST(ANY)`,
    /// `STRUCT({{"any", ANY}})`, the bare `DECIMAL`, ...: what a cast registered for a whole family matches on),
    /// through `duckdb_v2_factory_create_type_template`. `None` when the loading engine has no such entry (the
    /// upstream library); see [`LogicalType::template_available`].
    pub fn template(factory: Factory, id: TypeId) -> Result<Option<LogicalType>> {
        if !LogicalType::template_available() {
            return Ok(None);
        }
        let mut raw: duckdb_v2_logical_type_handle = std::ptr::null_mut();
        api_call!(duckdb_v2_factory_create_type_template(factory.as_raw(), id, &mut raw));
        Ok(Some(LogicalType { raw }))
    }

    /// Whether the loading engine builds type templates (`duckdb_v2_factory_create_type_template`, a newer engine
    /// entry).
    pub fn template_available() -> bool {
        crate::api::api().duckdb_v2_factory_create_type_template.is_some()
    }

    pub fn id(&self) -> Result<TypeId> {
        let mut id = type_id::INVALID;
        api_call!(duckdb_v2_logical_type_get_id(self.raw, &mut id));
        Ok(id)
    }

    /// The type's name: its alias when it has one, else the name of its id.
    pub fn name(&self) -> Result<String> {
        let mut name = duckdb_v2_str::EMPTY;
        api_call!(duckdb_v2_logical_type_get_name(self.raw, &mut name));
        Ok(str_to_string(name))
    }

    /// The SQL rendering, e.g. `DECIMAL(18,3)`.
    pub fn to_text(&self) -> Result<String> {
        render(|buffer, capacity, needed| {
            api_call!(duckdb_v2_logical_type_to_text(self.raw, buffer, capacity, needed));
            Ok(())
        })
    }

    pub fn param_count(&self) -> Result<usize> {
        let mut count: idx_t = 0;
        api_call!(duckdb_v2_logical_type_get_param_count(self.raw, &mut count));
        Ok(count as usize)
    }

    /// Parameter `index` as `(name, value)`; TYPE-valued parameters (the child of a LIST, the fields of a
    /// STRUCT) come back as TYPE values ([`Value::as_type`]).
    pub fn param(&self, index: usize) -> Result<(String, Value)> {
        let mut name = duckdb_v2_str::EMPTY;
        let mut value: duckdb_v2_value_handle = std::ptr::null_mut();
        api_call!(duckdb_v2_logical_type_get_param(self.raw, index as idx_t, &mut name, &mut value));
        // SAFETY(ffi L8.I1): the entry returns an owned copy of the parameter's value.
        Ok((str_to_string(name), unsafe { Value::from_raw(value) }))
    }

    pub fn is_equal(&self, other: &LogicalType) -> Result<bool> {
        let mut equal = false;
        api_call!(duckdb_v2_logical_type_is_equal(self.raw, other.raw, &mut equal));
        Ok(equal)
    }

    pub fn try_clone(&self) -> Result<LogicalType> {
        let mut raw: duckdb_v2_logical_type_handle = std::ptr::null_mut();
        api_call!(duckdb_v2_logical_type_copy(self.raw, &mut raw));
        Ok(LogicalType { raw })
    }

    /// The width in bytes of one element of a vector of this type - the C layout its data array holds (`i32` for
    /// INTEGER, the 16-byte `duckdb_v2_bytes` for the string types, `duckdb_v2_list_entry` for LIST / MAP, 2 / 4 /
    /// 8 / 16 for a DECIMAL by its width, 1 / 2 / 4 for an ENUM by its size; `LogicalType::GetInternalType`) - or
    /// `None` for a type whose vectors carry their rows in child vectors only (STRUCT, UNION, VARIANT, TUPLE, ARRAY).
    /// An error for a type that has no vectors (ANY, INVALID, ...).
    pub fn element_width(&self) -> Result<Option<usize>> {
        let id = self.id()?;
        Ok(Some(match id {
            type_id::BOOLEAN | type_id::TINYINT | type_id::UTINYINT => 1,
            type_id::SMALLINT | type_id::USMALLINT => 2,
            type_id::INTEGER | type_id::UINTEGER | type_id::DATE | type_id::FLOAT | type_id::SQLNULL => 4,
            type_id::BIGINT
            | type_id::UBIGINT
            | type_id::TIME
            | type_id::TIME_NS
            | type_id::TIME_TZ
            | type_id::TIMESTAMP
            | type_id::TIMESTAMP_SEC
            | type_id::TIMESTAMP_MS
            | type_id::TIMESTAMP_NS
            | type_id::TIMESTAMP_TZ
            | type_id::TIMESTAMP_TZ_NS
            | type_id::DOUBLE => 8,
            type_id::HUGEINT | type_id::UHUGEINT | type_id::UUID | type_id::INTERVAL => 16,
            type_id::VARCHAR
            | type_id::BLOB
            | type_id::BIT
            | DUCKDB_V2_LOGICAL_TYPE_ID_BIGNUM
            | DUCKDB_V2_LOGICAL_TYPE_ID_GEOMETRY
            | type_id::LIST
            | type_id::MAP => 16,
            type_id::DECIMAL => {
                let (_, width) = self.param(0)?;
                match width.as_u64()? {
                    0..=4 => 2,
                    5..=9 => 4,
                    10..=18 => 8,
                    _ => 16,
                }
            }
            type_id::ENUM => {
                let count = self.param_count()?;
                if count <= u8::MAX as usize {
                    1
                } else if count <= u16::MAX as usize {
                    2
                } else {
                    4
                }
            }
            type_id::STRUCT
            | type_id::UNION
            | type_id::ARRAY
            | DUCKDB_V2_LOGICAL_TYPE_ID_VARIANT
            | DUCKDB_V2_LOGICAL_TYPE_ID_TUPLE => return Ok(None),
            _ => {
                return Err(crate::Error::invalid_input(format!(
                    "a {} has no vector element layout",
                    self.to_text().unwrap_or_else(|_| format!("type {}", id.0))
                )));
            }
        }))
    }
}

impl Clone for LogicalType {
    fn clone(&self) -> LogicalType {
        self.try_clone().expect("copying a logical type")
    }
}

impl Drop for LogicalType {
    fn drop(&mut self) {
        api_destroy!(duckdb_v2_logical_type_destroy(&mut self.raw));
    }
}

impl fmt::Debug for LogicalType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.to_text() {
            Ok(text) => f.write_str(&text),
            Err(error) => write!(f, "<type: {error}>"),
        }
    }
}

impl PartialEq for LogicalType {
    fn eq(&self, other: &LogicalType) -> bool {
        self.is_equal(other).unwrap_or(false)
    }
}

/// A parameter name for `create_type_from_*`: `{NULL, 0}` marks a positional parameter.
fn param_name(name: &str) -> duckdb_v2_identifier_t {
    if name.is_empty() { duckdb_v2_str::EMPTY } else { str_arg(name) }
}
