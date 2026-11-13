//! Values (`duckdb_v2_value_handle`, owned).

use std::fmt;

use crate::Result;
use crate::context::Factory;
use crate::sys::*;
use crate::text::{render, str_arg, str_to_bytes, str_to_string};
use crate::types::LogicalType;

/// An owned SQL value.
pub struct Value {
    raw: duckdb_v2_value_handle,
}

// SAFETY(protocol L8.I8): (T5, L8.I1) an owned `duckdb_v2_value_handle` (`duckdb_v2.h`: "a logical type plus a typed payload";
// the engine side is a bare boxed value with no synchronization of its own). The value is built by its
// `create_*` constructor and this wrapper has no mutating entry point afterwards: every entry it calls reads the value
// (`get_*`, `is_null`, `to_string`, `get_child`; `cast` makes a new value), so a shared `&Value` is a shared read of an
// immutable value and the owned value moves between threads as a whole. `Sync` holds only as long as that stays true: a
// mutating entry would have to take `&mut self`.
unsafe impl Send for Value {}
unsafe impl Sync for Value {}

macro_rules! scalar_ctor {
    ($(#[$doc:meta])* $name:ident, $api:ident, $t:ty) => {
        $(#[$doc])*
        pub fn $name(factory: Factory, v: $t) -> Result<Value> {
            let mut raw: duckdb_v2_value_handle = std::ptr::null_mut();
            api_call!($api(factory.as_raw(), v, &mut raw));
            Ok(Value { raw })
        }
    };
}

macro_rules! scalar_getter {
    ($(#[$doc:meta])* $name:ident, $api:ident, $t:ty, $zero:expr) => {
        $(#[$doc])*
        pub fn $name(&self) -> Result<$t> {
            let mut out: $t = $zero;
            api_call!($api(self.raw, &mut out));
            Ok(out)
        }
    };
}

impl Value {
    /// # Safety
    /// (ffi L8.I1) `raw` must be a handle the C API returned for this type and that is still live. An owned wrapper
    /// destroys the handle on drop, so nothing else may destroy it; a borrowed (`Copy`) wrapper must not be used past
    /// the call that lent the handle.
    pub unsafe fn from_raw(raw: duckdb_v2_value_handle) -> Value {
        Value { raw }
    }

    pub fn as_raw(&self) -> duckdb_v2_value_handle {
        self.raw
    }

    pub fn into_raw(self) -> duckdb_v2_value_handle {
        let raw = self.raw;
        std::mem::forget(self);
        raw
    }

    scalar_ctor!(/// A BOOLEAN.
        boolean, duckdb_v2_value_create_bool, bool);
    scalar_ctor!(/// A TINYINT.
        tinyint, duckdb_v2_value_create_tinyint, i8);
    scalar_ctor!(/// A SMALLINT.
        smallint, duckdb_v2_value_create_smallint, i16);
    scalar_ctor!(/// An INTEGER.
        integer, duckdb_v2_value_create_int, i32);
    scalar_ctor!(/// A BIGINT.
        bigint, duckdb_v2_value_create_bigint, i64);
    scalar_ctor!(/// A UTINYINT.
        utinyint, duckdb_v2_value_create_utinyint, u8);
    scalar_ctor!(/// A USMALLINT.
        usmallint, duckdb_v2_value_create_usmallint, u16);
    scalar_ctor!(/// A UINTEGER.
        uinteger, duckdb_v2_value_create_uint, u32);
    scalar_ctor!(/// A UBIGINT.
        ubigint, duckdb_v2_value_create_ubigint, u64);
    scalar_ctor!(/// A FLOAT.
        float, duckdb_v2_value_create_float, f32);
    scalar_ctor!(/// A DOUBLE.
        double, duckdb_v2_value_create_double, f64);
    scalar_ctor!(/// A DATE (days since 1970-01-01).
        date, duckdb_v2_value_create_date, i32);
    scalar_ctor!(/// A TIME (microseconds since midnight).
        time, duckdb_v2_value_create_time, i64);
    scalar_ctor!(/// A TIMESTAMP (microseconds since the epoch).
        timestamp, duckdb_v2_value_create_timestamp, i64);
    scalar_ctor!(/// A TIMESTAMP WITH TIME ZONE (microseconds since the epoch).
        timestamp_tz, duckdb_v2_value_create_timestamp_tz, i64);
    scalar_ctor!(/// A TIME_NS (nanoseconds since midnight).
        time_ns, duckdb_v2_value_create_time_ns, i64);
    scalar_ctor!(/// A TIME WITH TIME ZONE (the engine's bits: time << 24 | encoded offset).
        time_tz, duckdb_v2_value_create_time_tz, u64);
    scalar_ctor!(/// A TIMESTAMP_S (seconds since the epoch).
        timestamp_sec, duckdb_v2_value_create_timestamp_sec, i64);
    scalar_ctor!(/// A TIMESTAMP_MS (milliseconds since the epoch).
        timestamp_ms, duckdb_v2_value_create_timestamp_ms, i64);
    scalar_ctor!(/// A TIMESTAMP_NS (nanoseconds since the epoch).
        timestamp_ns, duckdb_v2_value_create_timestamp_ns, i64);
    scalar_ctor!(/// A TIMESTAMP_NS WITH TIME ZONE (nanoseconds since the epoch).
        timestamp_tz_ns, duckdb_v2_value_create_timestamp_tz_ns, i64);

    /// A UHUGEINT.
    pub fn uhugeint(factory: Factory, v: u128) -> Result<Value> {
        let h = duckdb_v2_uhugeint_t { lower: v as u64, upper: (v >> 64) as u64 };
        let mut raw = std::ptr::null_mut();
        api_call!(duckdb_v2_value_create_uhugeint(factory.as_raw(), &h, &mut raw));
        Ok(Value { raw })
    }

    /// A UUID from its 128-bit representation (the engine's: the high bit flipped for ordering).
    pub fn uuid(factory: Factory, v: i128) -> Result<Value> {
        let h = duckdb_v2_hugeint_t { lower: v as u64, upper: (v >> 64) as i64 };
        let mut raw: duckdb_v2_value_handle = std::ptr::null_mut();
        api_call!(duckdb_v2_value_create_uuid(factory.as_raw(), &h, &mut raw));
        Ok(Value { raw })
    }

    /// A VARCHAR.
    pub fn varchar(factory: Factory, text: &str) -> Result<Value> {
        let text = str_arg(text);
        let mut raw: duckdb_v2_value_handle = std::ptr::null_mut();
        api_call!(duckdb_v2_value_create_varchar(factory.as_raw(), &text, &mut raw));
        Ok(Value { raw })
    }

    /// A BLOB.
    pub fn blob(factory: Factory, bytes: &[u8]) -> Result<Value> {
        let bytes = duckdb_v2_str { ptr: bytes.as_ptr() as *const std::ffi::c_char, len: bytes.len() as idx_t };
        let mut raw: duckdb_v2_value_handle = std::ptr::null_mut();
        api_call!(duckdb_v2_value_create_blob(factory.as_raw(), &bytes, &mut raw));
        Ok(Value { raw })
    }

    /// A HUGEINT.
    pub fn hugeint(factory: Factory, v: i128) -> Result<Value> {
        let h = duckdb_v2_hugeint_t { lower: v as u64, upper: (v >> 64) as i64 };
        let mut raw: duckdb_v2_value_handle = std::ptr::null_mut();
        api_call!(duckdb_v2_value_create_hugeint(factory.as_raw(), &h, &mut raw));
        Ok(Value { raw })
    }

    /// A DECIMAL(width, scale) from its unscaled integer.
    pub fn decimal(factory: Factory, unscaled: i128, width: u8, scale: u8) -> Result<Value> {
        let h = duckdb_v2_hugeint_t { lower: unscaled as u64, upper: (unscaled >> 64) as i64 };
        let mut raw: duckdb_v2_value_handle = std::ptr::null_mut();
        api_call!(duckdb_v2_value_create_decimal(factory.as_raw(), &h, width, scale, &mut raw));
        Ok(Value { raw })
    }

    /// An INTERVAL.
    pub fn interval(factory: Factory, months: i32, days: i32, micros: i64) -> Result<Value> {
        let i = duckdb_v2_interval_t { months, days, micros };
        let mut raw: duckdb_v2_value_handle = std::ptr::null_mut();
        api_call!(duckdb_v2_value_create_interval(factory.as_raw(), &i, &mut raw));
        Ok(Value { raw })
    }

    /// NULL of the given type.
    pub fn null(factory: Factory, type_: &LogicalType) -> Result<Value> {
        let mut raw: duckdb_v2_value_handle = std::ptr::null_mut();
        api_call!(duckdb_v2_value_create_null(factory.as_raw(), type_.as_raw(), &mut raw));
        Ok(Value { raw })
    }

    /// A LIST of `child_type` holding `values`.
    pub fn list(factory: Factory, child_type: &LogicalType, values: &[Value]) -> Result<Value> {
        let raws: Vec<duckdb_v2_value_handle> = values.iter().map(|v| v.raw).collect();
        let mut raw: duckdb_v2_value_handle = std::ptr::null_mut();
        api_call!(duckdb_v2_value_create_list(
            factory.as_raw(),
            child_type.as_raw(),
            if raws.is_empty() { std::ptr::null() } else { raws.as_ptr() },
            raws.len() as idx_t,
            &mut raw
        ));
        Ok(Value { raw })
    }

    /// A STRUCT with the given fields.
    pub fn struct_(factory: Factory, fields: &[(&str, Value)]) -> Result<Value> {
        let names: Vec<duckdb_v2_identifier_t> = fields.iter().map(|(n, _)| str_arg(n)).collect();
        let raws: Vec<duckdb_v2_value_handle> = fields.iter().map(|(_, v)| v.raw).collect();
        let mut raw: duckdb_v2_value_handle = std::ptr::null_mut();
        api_call!(duckdb_v2_value_create_struct(
            factory.as_raw(),
            names.as_ptr(),
            raws.as_ptr(),
            raws.len() as idx_t,
            &mut raw
        ));
        Ok(Value { raw })
    }

    /// A MAP from parallel key and value lists.
    pub fn map(
        factory: Factory,
        key_type: &LogicalType,
        value_type: &LogicalType,
        keys: &[Value],
        values: &[Value],
    ) -> Result<Value> {
        let key_raws: Vec<duckdb_v2_value_handle> = keys.iter().map(|v| v.raw).collect();
        let value_raws: Vec<duckdb_v2_value_handle> = values.iter().map(|v| v.raw).collect();
        let mut raw: duckdb_v2_value_handle = std::ptr::null_mut();
        api_call!(duckdb_v2_value_create_map(
            factory.as_raw(),
            key_type.as_raw(),
            value_type.as_raw(),
            if key_raws.is_empty() { std::ptr::null() } else { key_raws.as_ptr() },
            if value_raws.is_empty() { std::ptr::null() } else { value_raws.as_ptr() },
            key_raws.len() as idx_t,
            &mut raw
        ));
        Ok(Value { raw })
    }

    /// A TYPE value: what parameterised types take as their type parameters.
    pub fn type_value(factory: Factory, type_: &LogicalType) -> Result<Value> {
        let mut raw: duckdb_v2_value_handle = std::ptr::null_mut();
        api_call!(duckdb_v2_value_create_type(factory.as_raw(), type_.as_raw(), &mut raw));
        Ok(Value { raw })
    }

    pub fn is_null(&self) -> Result<bool> {
        let mut null = false;
        api_call!(duckdb_v2_value_is_null(self.raw, &mut null));
        Ok(null)
    }

    /// The type of the value (`duckdb_v2_value_get_logical_type`).
    pub fn logical_type(&self) -> Result<LogicalType> {
        Ok(api_call_out!(duckdb_v2_value_get_logical_type(self.raw) -> LogicalType::from_raw))
    }

    /// The type a TYPE value holds (`duckdb_v2_value_get_type`; an error for any other value).
    pub fn as_type(&self) -> Result<LogicalType> {
        Ok(api_call_out!(duckdb_v2_value_get_type(self.raw) -> LogicalType::from_raw))
    }

    scalar_getter!(/// The BOOLEAN.
        as_bool, duckdb_v2_value_get_bool, bool, false);
    scalar_getter!(/// The TINYINT.
        as_i8, duckdb_v2_value_get_tinyint, i8, 0);
    scalar_getter!(/// The SMALLINT.
        as_i16, duckdb_v2_value_get_smallint, i16, 0);
    scalar_getter!(/// The INTEGER.
        as_i32, duckdb_v2_value_get_int, i32, 0);
    scalar_getter!(/// The BIGINT.
        as_i64, duckdb_v2_value_get_bigint, i64, 0);
    scalar_getter!(/// The UTINYINT.
        as_u8, duckdb_v2_value_get_utinyint, u8, 0);
    scalar_getter!(/// The USMALLINT.
        as_u16, duckdb_v2_value_get_usmallint, u16, 0);
    scalar_getter!(/// The UINTEGER.
        as_u32, duckdb_v2_value_get_uint, u32, 0);
    scalar_getter!(/// The UBIGINT.
        as_u64, duckdb_v2_value_get_ubigint, u64, 0);
    scalar_getter!(/// The FLOAT.
        as_f32, duckdb_v2_value_get_float, f32, 0.0);
    scalar_getter!(/// The DOUBLE.
        as_f64, duckdb_v2_value_get_double, f64, 0.0);
    scalar_getter!(/// The DATE (days since 1970-01-01).
        as_date, duckdb_v2_value_get_date, i32, 0);
    scalar_getter!(/// The TIMESTAMP (microseconds since the epoch).
        as_timestamp, duckdb_v2_value_get_timestamp, i64, 0);
    scalar_getter!(/// The TIMESTAMP WITH TIME ZONE (microseconds since the epoch).
        as_timestamp_tz, duckdb_v2_value_get_timestamp_tz, i64, 0);
    scalar_getter!(/// The TIMESTAMP_NS WITH TIME ZONE (nanoseconds since the epoch).
        as_timestamp_tz_ns, duckdb_v2_value_get_timestamp_tz_ns, i64, 0);
    scalar_getter!(/// The TIME WITH TIME ZONE (the engine's bits: time << 24 | encoded offset).
        as_time_tz, duckdb_v2_value_get_time_tz, u64, 0);

    scalar_getter!(/// The TIME (microseconds since midnight).
        as_time, duckdb_v2_value_get_time, i64, 0);
    scalar_getter!(/// The TIME_NS (nanoseconds since midnight).
        as_time_ns, duckdb_v2_value_get_time_ns, i64, 0);
    scalar_getter!(/// The TIMESTAMP_S (seconds since the epoch).
        as_timestamp_sec, duckdb_v2_value_get_timestamp_sec, i64, 0);
    scalar_getter!(/// The TIMESTAMP_MS (milliseconds since the epoch).
        as_timestamp_ms, duckdb_v2_value_get_timestamp_ms, i64, 0);
    scalar_getter!(/// The TIMESTAMP_NS (nanoseconds since the epoch).
        as_timestamp_ns, duckdb_v2_value_get_timestamp_ns, i64, 0);

    /// The HUGEINT.
    pub fn as_i128(&self) -> Result<i128> {
        let mut h = duckdb_v2_hugeint_t { lower: 0, upper: 0 };
        api_call!(duckdb_v2_value_get_hugeint(self.raw, &mut h));
        Ok(((h.upper as i128) << 64) | h.lower as i128)
    }

    /// The UHUGEINT.
    pub fn as_u128(&self) -> Result<u128> {
        let mut h = duckdb_v2_uhugeint_t { lower: 0, upper: 0 };
        api_call!(duckdb_v2_value_get_uhugeint(self.raw, &mut h));
        Ok(((h.upper as u128) << 64) | h.lower as u128)
    }

    /// The UUID as its 128-bit representation (the engine's: the high bit flipped for ordering).
    pub fn as_uuid(&self) -> Result<i128> {
        let mut h = duckdb_v2_hugeint_t { lower: 0, upper: 0 };
        api_call!(duckdb_v2_value_get_uuid(self.raw, &mut h));
        Ok(((h.upper as i128) << 64) | h.lower as i128)
    }

    /// The DECIMAL as `(unscaled value, width, scale)`.
    pub fn as_decimal(&self) -> Result<(i128, u8, u8)> {
        let mut h = duckdb_v2_hugeint_t { lower: 0, upper: 0 };
        let (mut width, mut scale) = (0u8, 0u8);
        api_call!(duckdb_v2_value_get_decimal(self.raw, &mut h, &mut width, &mut scale));
        Ok((((h.upper as i128) << 64) | h.lower as i128, width, scale))
    }

    /// The VARCHAR.
    pub fn as_str(&self) -> Result<String> {
        let mut text = duckdb_v2_str::EMPTY;
        api_call!(duckdb_v2_value_get_varchar(self.raw, &mut text));
        Ok(str_to_string(text))
    }

    /// The BLOB.
    pub fn as_blob(&self) -> Result<Vec<u8>> {
        let mut bytes = duckdb_v2_str::EMPTY;
        api_call!(duckdb_v2_value_get_blob(self.raw, &mut bytes));
        Ok(str_to_bytes(bytes))
    }

    /// The INTERVAL as `(months, days, micros)`.
    pub fn as_interval(&self) -> Result<(i32, i32, i64)> {
        let mut i = duckdb_v2_interval_t { months: 0, days: 0, micros: 0 };
        api_call!(duckdb_v2_value_get_interval(self.raw, &mut i));
        Ok((i.months, i.days, i.micros))
    }

    /// The number of children of a nested value (LIST elements, STRUCT fields, MAP entries).
    pub fn child_count(&self) -> Result<usize> {
        let mut count: idx_t = 0;
        api_call!(duckdb_v2_value_get_child_count(self.raw, &mut count));
        Ok(count as usize)
    }

    pub fn child(&self, index: usize) -> Result<Value> {
        let mut raw: duckdb_v2_value_handle = std::ptr::null_mut();
        api_call!(duckdb_v2_value_get_child(self.raw, index as idx_t, &mut raw));
        Ok(Value { raw })
    }

    /// Casts to `type_` through the engine's cast functions.
    pub fn cast(&self, factory: Factory, type_: &LogicalType) -> Result<Value> {
        let mut raw: duckdb_v2_value_handle = std::ptr::null_mut();
        api_call!(duckdb_v2_value_cast(factory.as_raw(), self.raw, type_.as_raw(), &mut raw));
        Ok(Value { raw })
    }

    /// The SQL rendering (`Value::ToString`).
    pub fn to_text(&self) -> Result<String> {
        render(|buffer, capacity, needed| {
            api_call!(duckdb_v2_value_to_string(self.raw, buffer, capacity, needed));
            Ok(())
        })
    }
}

impl Drop for Value {
    fn drop(&mut self) {
        api_destroy!(duckdb_v2_value_destroy(&mut self.raw));
    }
}

impl fmt::Debug for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.to_text() {
            Ok(text) => f.write_str(&text),
            Err(error) => write!(f, "<value: {error}>"),
        }
    }
}
