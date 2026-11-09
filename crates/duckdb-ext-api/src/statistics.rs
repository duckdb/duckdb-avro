//! Column statistics (`duckdb_v2_statistics_handle`): what a table function knows about the values of a column
//! without reading them, reported from its statistics callback (`TableStatistics`); and the borrowed [`Stats`] a
//! stats callback (`ScalarStats`, `AggregateStats`, `TableStats`) reads or narrows.

use crate::Result;
use crate::context::Factory;
use crate::sys::*;
use crate::text::bytes_arg;
use crate::types::LogicalType;
use crate::value::Value;

/// The statistics of an expression or a column (`duckdb_v2_stats_handle`, the C++ wrapper's `Stats`): what values it
/// can produce. A function's stats callback reads the statistics of its arguments and narrows those of its result,
/// and the engine uses them to simplify the query, e.g. folding `x IS NULL` to false when `x` cannot be NULL.
///
/// Every statistic states what can occur: `true` means a kind of value may be present, `false` guarantees it is
/// absent. The engine relies on the guarantees, so claiming less than the data holds produces wrong query results.
/// Borrowed: valid only for the duration of the callback that handed it out. Unstable API.
#[derive(Clone, Copy)]
pub struct Stats {
    raw: duckdb_v2_stats_handle,
}

impl Stats {
    /// Wraps a handle a callback info lent out.
    ///
    /// # Safety
    /// (ffi L8.I1) `raw` must be a live `duckdb_v2_stats_handle` the engine lent to the running callback; the wrapper
    /// is `Copy` and must not be used past that callback.
    pub unsafe fn from_raw(raw: duckdb_v2_stats_handle) -> Stats {
        Stats { raw }
    }

    pub fn as_raw(&self) -> duckdb_v2_stats_handle {
        self.raw
    }

    /// Whether a value can be NULL. False guarantees every value is non-NULL.
    pub fn can_have_null(&self) -> Result<bool> {
        let mut out = false;
        api_call!(duckdb_v2_stats_can_have_null(self.raw, &mut out));
        Ok(out)
    }

    /// Whether a value can be valid, i.e. non-NULL. False guarantees every value is NULL. When both this and
    /// [`Stats::can_have_null`] are false, there are no values at all.
    pub fn can_have_valid(&self) -> Result<bool> {
        let mut out = false;
        api_call!(duckdb_v2_stats_can_have_valid(self.raw, &mut out));
        Ok(out)
    }

    /// Sets whether a value can be NULL. Setting false is a guarantee the engine relies on; setting true is always
    /// safe. Fails (`InvalidInput`) when the statistics are read-only, such as those of an argument.
    pub fn set_can_have_null(&self, value: bool) -> Result<()> {
        api_call!(duckdb_v2_stats_set_can_have_null(self.raw, value));
        Ok(())
    }

    /// Sets whether a value can be valid, i.e. non-NULL. Setting false is a guarantee the engine relies on; setting
    /// true is always safe. Fails (`InvalidInput`) when the statistics are read-only, such as those of an argument.
    pub fn set_can_have_valid(&self, value: bool) -> Result<()> {
        api_call!(duckdb_v2_stats_set_can_have_valid(self.raw, value));
        Ok(())
    }
}

/// Owned statistics of a column: created unknown (claiming nothing) and narrowed with the setters.
pub struct Statistics {
    raw: duckdb_v2_statistics_handle,
}

// SAFETY(protocol L8.I8): (T5, L8.I1) an owned `duckdb_v2_statistics_handle` (`duckdb_v2.h`: `BaseStatistics` that
// "start out unknown ... are narrowed with the `duckdb_v2_statistics_set_*()` functions, combined with `_merge()`");
// every narrowing entry takes `&mut self` and the one read (`logical_type`) `&self`, so a shared `&Statistics` is a
// shared read of a plain value and the owned statistics move between threads as a whole.
unsafe impl Send for Statistics {}
unsafe impl Sync for Statistics {}

impl Statistics {
    /// Unknown statistics of a column of `type_`.
    pub fn new(factory: Factory, type_: &LogicalType) -> Result<Statistics> {
        let mut raw = std::ptr::null_mut();
        api_call!(duckdb_v2_statistics_create(factory.as_raw(), type_.as_raw(), &mut raw));
        Ok(Statistics { raw })
    }

    /// The statistics of a shredded VARIANT column, from the statistics of its shredded layout (of the type
    /// [`Statistics::variant_shredding_type`] derives).
    pub fn variant_shredded(factory: Factory, shredded: &Statistics) -> Result<Statistics> {
        let mut raw: duckdb_v2_statistics_handle = std::ptr::null_mut();
        api_call!(duckdb_v2_statistics_create_variant_shredded(factory.as_raw(), shredded.raw, &mut raw));
        Ok(Statistics { raw })
    }

    /// The type of the shredded layout of a VARIANT column shredded on `structured_type`: every level wrapped in
    /// `STRUCT(typed_value, untyped_value_index UINTEGER)`.
    pub fn variant_shredding_type(factory: Factory, structured_type: &LogicalType) -> Result<LogicalType> {
        Ok(
            api_call_out!(duckdb_v2_statistics_variant_shredding_type(factory.as_raw(), structured_type.as_raw()) -> LogicalType::from_raw),
        )
    }

    /// Takes ownership of a handle the API returned.
    /// # Safety
    /// (ffi L8.I1) `raw` must be a handle the C API returned for this type and that is still live. An owned wrapper
    /// destroys the handle on drop, so nothing else may destroy it; a borrowed (`Copy`) wrapper must not be used past
    /// the call that lent the handle.
    pub unsafe fn from_raw(raw: duckdb_v2_statistics_handle) -> Statistics {
        Statistics { raw }
    }

    pub fn as_raw(&self) -> duckdb_v2_statistics_handle {
        self.raw
    }

    /// The type of the column the statistics describe.
    pub fn logical_type(&self) -> Result<LogicalType> {
        Ok(api_call_out!(duckdb_v2_statistics_get_type(self.raw) -> LogicalType::from_raw))
    }

    /// Whether the column can hold NULL values.
    pub fn set_has_null(&mut self, has_null: bool) -> Result<&mut Self> {
        api_call!(duckdb_v2_statistics_set_has_null(self.raw, has_null));
        Ok(self)
    }

    /// Whether the column can hold non-NULL values.
    pub fn set_has_no_null(&mut self, has_no_null: bool) -> Result<&mut Self> {
        api_call!(duckdb_v2_statistics_set_has_no_null(self.raw, has_no_null));
        Ok(self)
    }

    /// The minimum of a numeric column (cast to its type; NULL removes the bound).
    pub fn set_min(&mut self, value: &Value) -> Result<&mut Self> {
        api_call!(duckdb_v2_statistics_set_min(self.raw, value.as_raw()));
        Ok(self)
    }

    /// The maximum of a numeric column (cast to its type; NULL removes the bound).
    pub fn set_max(&mut self, value: &Value) -> Result<&mut Self> {
        api_call!(duckdb_v2_statistics_set_max(self.raw, value.as_raw()));
        Ok(self)
    }

    /// The minimum of a VARCHAR / BLOB column: the value itself (`is_exact`) or a truncated prefix of it.
    pub fn set_string_min(&mut self, value: &[u8], is_exact: bool) -> Result<&mut Self> {
        let value = bytes_arg(value);
        api_call!(duckdb_v2_statistics_set_string_min(self.raw, &value, is_exact));
        Ok(self)
    }

    /// The maximum of a VARCHAR / BLOB column: the value itself (`is_exact`) or a truncated bound of it.
    pub fn set_string_max(&mut self, value: &[u8], is_exact: bool) -> Result<&mut Self> {
        let value = bytes_arg(value);
        api_call!(duckdb_v2_statistics_set_string_max(self.raw, &value, is_exact));
        Ok(self)
    }

    /// The statistics of a column whose every value is `value`.
    pub fn set_constant(&mut self, value: &Value) -> Result<&mut Self> {
        api_call!(duckdb_v2_statistics_set_constant(self.raw, value.as_raw()));
        Ok(self)
    }

    /// The estimated number of distinct values (0: unknown).
    pub fn set_distinct_count(&mut self, count: usize) -> Result<&mut Self> {
        api_call!(duckdb_v2_statistics_set_distinct_count(self.raw, count as idx_t));
        Ok(self)
    }

    /// The statistics of a child of a LIST / ARRAY (child 0) or STRUCT column; `None` resets it to unknown.
    pub fn set_child(&mut self, child_index: usize, child: Option<&Statistics>) -> Result<&mut Self> {
        let child = child.map(|c| c.raw).unwrap_or(std::ptr::null_mut());
        api_call!(duckdb_v2_statistics_set_child(self.raw, child_index as idx_t, child));
        Ok(self)
    }

    /// Widens these statistics to hold for the values of `other` too (same type).
    pub fn merge(&mut self, other: &Statistics) -> Result<&mut Self> {
        api_call!(duckdb_v2_statistics_merge(self.raw, other.raw));
        Ok(self)
    }

    /// The bounding box of a GEOMETRY column; the Z / M bounds are left unknown when `None`.
    pub fn set_geometry_extent(
        &mut self,
        x: (f64, f64),
        y: (f64, f64),
        z: Option<(f64, f64)>,
        m: Option<(f64, f64)>,
    ) -> Result<&mut Self> {
        api_call!(duckdb_v2_statistics_set_geometry_extent(
            self.raw,
            x.0,
            x.1,
            y.0,
            y.1,
            z.is_some(),
            z.map_or(0.0, |z| z.0),
            z.map_or(0.0, |z| z.1),
            m.is_some(),
            m.map_or(0.0, |m| m.0),
            m.map_or(0.0, |m| m.1)
        ));
        Ok(self)
    }

    /// The ISO WKB geometry types a GEOMETRY column holds (empty: any).
    pub fn set_geometry_types(&mut self, wkb_types: &[i32]) -> Result<&mut Self> {
        api_call!(duckdb_v2_statistics_set_geometry_types(self.raw, wkb_types.as_ptr(), wkb_types.len() as idx_t));
        Ok(self)
    }
}

impl Drop for Statistics {
    fn drop(&mut self) {
        api_destroy!(duckdb_v2_statistics_destroy(&mut self.raw));
    }
}
