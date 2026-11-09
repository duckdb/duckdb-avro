//! Bound expressions (borrowed), as handed to a filter-pushdown callback, and owned copies of them.

use crate::Result;
use crate::context::Context;
use crate::qname::QualifiedName;
use crate::statistics::Statistics;
use crate::sys::*;
use crate::text::str_to_string;
use crate::types::LogicalType;
use crate::value::Value;

/// `DUCKDB_V2_EXPRESSION_TYPE`.
pub type ExpressionType = DUCKDB_V2_EXPRESSION_TYPE;
/// `DUCKDB_V2_FILTER_PROPAGATE_RESULT`: what checking a predicate against statistics tells
/// (`NO_PRUNING_POSSIBLE` / `ALWAYS_TRUE` / `ALWAYS_FALSE` / `TRUE_OR_NULL` / `FALSE_OR_NULL`).
pub type FilterPropagateResult = DUCKDB_V2_FILTER_PROPAGATE_RESULT;

/// A borrowed bound expression tree.
#[derive(Clone, Copy)]
pub struct Expression {
    raw: duckdb_v2_expression_handle,
}

impl Expression {
    /// # Safety
    /// (ffi L8.I1) `raw` must be a handle the C API returned for this type and that is still live. An owned wrapper
    /// destroys the handle on drop, so nothing else may destroy it; a borrowed (`Copy`) wrapper must not be used past
    /// the call that lent the handle.
    pub unsafe fn from_raw(raw: duckdb_v2_expression_handle) -> Expression {
        Expression { raw }
    }

    pub fn as_raw(&self) -> duckdb_v2_expression_handle {
        self.raw
    }

    pub fn expression_type(&self) -> Result<ExpressionType> {
        let mut et = DUCKDB_V2_EXPRESSION_TYPE_INVALID;
        api_call!(duckdb_v2_expression_get_type(self.raw, &mut et));
        Ok(et)
    }

    pub fn return_type(&self) -> Result<LogicalType> {
        Ok(api_call_out!(duckdb_v2_expression_get_return_type(self.raw) -> LogicalType::from_raw))
    }

    pub fn child_count(&self) -> Result<usize> {
        let mut count: idx_t = 0;
        api_call!(duckdb_v2_expression_get_child_count(self.raw, &mut count));
        Ok(count as usize)
    }

    pub fn child(&self, index: usize) -> Result<Expression> {
        let mut raw = std::ptr::null_mut();
        api_call!(duckdb_v2_expression_get_child(self.raw, index as idx_t, &mut raw));
        Ok(Expression { raw })
    }

    /// The value of a constant expression.
    pub fn constant_value(&self) -> Result<Value> {
        Ok(api_call_out!(duckdb_v2_expression_constant_get_value(self.raw) -> Value::from_raw))
    }

    /// The column index of a column reference (into the scanned columns).
    pub fn column_index(&self) -> Result<usize> {
        let mut index: idx_t = 0;
        api_call!(duckdb_v2_expression_column_ref_get_index(self.raw, &mut index));
        Ok(index as usize)
    }

    /// The name of a function expression.
    pub fn function_name(&self) -> Result<String> {
        let mut name = duckdb_v2_str::EMPTY;
        api_call!(duckdb_v2_expression_function_get_name(self.raw, &mut name));
        Ok(str_to_string(name))
    }

    /// The qualified name of a function expression.
    pub fn function_qname(&self) -> Result<QualifiedName> {
        Ok(api_call_out!(duckdb_v2_expression_function_get_qname(self.raw) -> QualifiedName::from_raw))
    }

    /// The mode of a cast expression.
    pub fn cast_mode(&self) -> Result<DUCKDB_V2_CAST_MODE> {
        let mut mode = DUCKDB_V2_CAST_MODE_NORMAL;
        api_call!(duckdb_v2_expression_cast_get_mode(self.raw, &mut mode));
        Ok(mode)
    }

    /// An owned copy of the expression (`duckdb_v2_expression_copy`), kept past the callback that lent it. Rust
    /// engine only: an error for a library without the entry (see [`OwnedExpression::available`]).
    pub fn try_clone(&self) -> Result<OwnedExpression> {
        let mut raw: duckdb_v2_expression_handle = std::ptr::null_mut();
        api_call!(duckdb_v2_expression_copy(self.raw, &mut raw));
        Ok(OwnedExpression { raw })
    }

    /// Checks the predicate against the statistics of its column (`FilterPropagateExpression`,
    /// `duckdb_v2_expression_check_statistics`): whether the rows the statistics describe can satisfy it.
    pub fn check_statistics(&self, context: &Context, statistics: &Statistics) -> Result<FilterPropagateResult> {
        let mut result = DUCKDB_V2_FILTER_PROPAGATE_RESULT_NO_PRUNING_POSSIBLE;
        api_call!(duckdb_v2_expression_check_statistics(context.as_raw(), self.raw, statistics.as_raw(), &mut result));
        Ok(result)
    }
}

/// An owned copy of a bound expression ([`Expression::try_clone`]), destroyed when dropped. Expressions are
/// immutable values without thread affinity.
#[derive(Debug)]
pub struct OwnedExpression {
    raw: duckdb_v2_expression_handle,
}

// SAFETY(protocol L8.I8): (T5, L8.I1) an owned copy (`duckdb_v2_expression_copy`) of a bound expression (`duckdb_v2.h`:
// expression handles are "Read-only"); every entry the wrapper calls on it reads (`get_type`, `get_child`, the per-kind
// getters, `check_statistics`), so a shared `&OwnedExpression` is a shared read of an immutable value, and the copy
// moves between threads as a whole.
unsafe impl Send for OwnedExpression {}
unsafe impl Sync for OwnedExpression {}

impl OwnedExpression {
    /// Whether the loading engine copies expressions (`duckdb_v2_expression_copy` + `_destroy`) and checks them
    /// against statistics (`duckdb_v2_expression_check_statistics`).
    pub fn available() -> bool {
        let api = crate::api::api();
        api.duckdb_v2_expression_copy.is_some()
            && api.duckdb_v2_expression_destroy.is_some()
            && api.duckdb_v2_expression_check_statistics.is_some()
    }

    pub fn as_raw(&self) -> duckdb_v2_expression_handle {
        self.raw
    }

    /// The copy as a borrowed expression (valid as long as `self`).
    pub fn as_expression(&self) -> Expression {
        Expression { raw: self.raw }
    }
}

impl PartialEq for OwnedExpression {
    /// Two owned expressions are equal when they are the same copy.
    fn eq(&self, other: &OwnedExpression) -> bool {
        std::ptr::eq(self.raw, other.raw)
    }
}

impl Drop for OwnedExpression {
    fn drop(&mut self) {
        api_destroy!(duckdb_v2_expression_destroy(&mut self.raw));
    }
}
