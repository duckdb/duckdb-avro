//! Cast functions between two types.

use std::ffi::c_void;

use crate::Result;
use crate::context::{Context, Factory};
use crate::opaque::Opaque;
use crate::sys::*;
use crate::types::LogicalType;
use crate::vector::Vector;

/// `DUCKDB_V2_CAST_MODE` (`NORMAL` errors on a failing row, `TRY` makes it NULL).
pub type CastMode = DUCKDB_V2_CAST_MODE;

/// Binds the cast for a call site (`bind_cast_function_t`): sees the source and target types, stores bind data.
pub trait CastBind {
    fn bind(info: &CastBindInfo, context: &Context) -> Result<()>;
}

/// Converts the input vector into the output vector.
pub trait CastExec {
    fn exec(info: &CastExecInfo, context: &Context) -> Result<()>;
}

trampoline!(bind_trampoline, CastBind::bind, CastBindInfo, duckdb_v2_cast_function_bind_info_handle);
trampoline!(exec_trampoline, CastExec::exec, CastExecInfo, duckdb_v2_cast_function_exec_info_handle);

/// A cast function under construction (owned until registered).
pub struct CastFunction {
    raw: duckdb_v2_cast_function_handle,
}

impl CastFunction {
    pub fn new(factory: Factory) -> Result<CastFunction> {
        let mut raw = std::ptr::null_mut();
        api_call!(duckdb_v2_cast_function_create(factory.as_raw(), &mut raw));
        Ok(CastFunction { raw })
    }

    pub fn as_raw(&self) -> duckdb_v2_cast_function_handle {
        self.raw
    }

    pub fn set_source_type(&self, type_: &LogicalType) -> Result<&Self> {
        api_call!(duckdb_v2_cast_function_set_source_type(self.raw, type_.as_raw()));
        Ok(self)
    }

    pub fn set_target_type(&self, type_: &LogicalType) -> Result<&Self> {
        api_call!(duckdb_v2_cast_function_set_target_type(self.raw, type_.as_raw()));
        Ok(self)
    }

    /// The cost of using this cast implicitly (negative: never implicitly).
    pub fn set_implicit_cast_cost(&self, cost: i64) -> Result<&Self> {
        api_call!(duckdb_v2_cast_function_set_implicit_cast_cost(self.raw, cost));
        Ok(self)
    }

    pub fn set_user_data<T: 'static>(&self, data: T) -> Result<&Self> {
        let mut opaque = Opaque::new(data);
        api_call!(duckdb_v2_cast_function_set_user_data(self.raw, &mut opaque));
        Ok(self)
    }

    pub fn set_exec<F: CastExec>(&self) -> Result<&Self> {
        api_call!(duckdb_v2_cast_function_set_exec_callback(self.raw, Some(exec_trampoline::<F>)));
        Ok(self)
    }

    /// The bind callback (`BoundCastInfo` from a `bind_cast_function_t`). newer engines only: an error for a library
    /// without the entry, see [`CastFunction::bind_available`].
    pub fn set_bind<F: CastBind>(&self) -> Result<&Self> {
        api_call!(duckdb_v2_cast_function_set_bind_callback(self.raw, Some(bind_trampoline::<F>)));
        Ok(self)
    }

    /// Whether the loading library binds casts (the upstream C API has no cast bind callback).
    pub fn bind_available() -> bool {
        crate::api::api().duckdb_v2_cast_function_set_bind_callback.is_some()
    }
}

/// The bind callback's info.
#[derive(Clone, Copy)]
pub struct CastBindInfo {
    raw: duckdb_v2_cast_function_bind_info_handle,
}

impl CastBindInfo {
    pub fn source_type(&self) -> Result<LogicalType> {
        Ok(api_call_out!(duckdb_v2_cast_function_bind_get_source_type(self.raw) -> LogicalType::from_raw))
    }

    pub fn target_type(&self) -> Result<LogicalType> {
        Ok(api_call_out!(duckdb_v2_cast_function_bind_get_target_type(self.raw) -> LogicalType::from_raw))
    }

    opaque_getter!(user_data, duckdb_v2_cast_function_bind_get_user_data);

    /// Stores the bind data the exec callback reads back with [`CastExecInfo::bind_data`].
    pub fn set_bind_data<T: 'static>(&self, data: T) -> Result<()> {
        let mut opaque = Opaque::new(data);
        api_call!(duckdb_v2_cast_function_bind_set_bind_data(self.raw, &mut opaque));
        Ok(())
    }
}

impl Drop for CastFunction {
    fn drop(&mut self) {
        api_destroy!(duckdb_v2_cast_function_destroy(&mut self.raw));
    }
}

/// The exec callback's info.
#[derive(Clone, Copy)]
pub struct CastExecInfo {
    raw: duckdb_v2_cast_function_exec_info_handle,
}

impl CastExecInfo {
    opaque_getter!(user_data, duckdb_v2_cast_function_exec_get_user_data);

    /// The bind data the bind callback stored (`None` without a bind callback or a library without the entry).
    pub fn bind_data<T>(&self) -> Result<Option<&T>> {
        if crate::api::api().duckdb_v2_cast_function_exec_get_bind_data.is_none() {
            return Ok(None);
        }
        let mut data: *mut c_void = std::ptr::null_mut();
        api_call!(duckdb_v2_cast_function_exec_get_bind_data(self.raw, &mut data));
        // SAFETY(ffi L8.I1): the slot holds what the bind callback stored through `set_bind_data` for the `T` the
        // caller names, or is empty.
        Ok(unsafe { Opaque::get::<T>(data) })
    }

    pub fn row_count(&self) -> Result<usize> {
        let mut count: idx_t = 0;
        api_call!(duckdb_v2_cast_function_exec_get_row_count(self.raw, &mut count));
        Ok(count as usize)
    }

    pub fn mode(&self) -> Result<CastMode> {
        let mut mode = DUCKDB_V2_CAST_MODE_NORMAL;
        api_call!(duckdb_v2_cast_function_exec_get_mode(self.raw, &mut mode));
        Ok(mode)
    }

    /// Makes the error the callback reports abort the query in every cast mode, `CAST_MODE_TRY` included: the
    /// exception a C++ cast throws (e.g. a MAP key that cannot be NULL), where a reported error is otherwise the
    /// `CastParameters::error_message` a try cast swallows. newer engines only; a no-op for a library without the entry.
    pub fn set_raise_error(&self, raise_error: bool) -> Result<()> {
        if crate::api::api().duckdb_v2_cast_function_exec_set_raise_error.is_none() {
            return Ok(());
        }
        api_call!(duckdb_v2_cast_function_exec_set_raise_error(self.raw, raise_error));
        Ok(())
    }

    pub fn input(&self) -> Result<Vector> {
        Ok(api_call_out!(duckdb_v2_cast_function_exec_get_input(self.raw) -> Vector::from_raw))
    }

    pub fn output(&self) -> Result<Vector> {
        Ok(api_call_out!(duckdb_v2_cast_function_exec_get_output(self.raw) -> Vector::from_raw))
    }
}
