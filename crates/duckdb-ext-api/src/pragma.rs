//! Pragma functions (`PRAGMA name(args)` / `CALL name(args)`): either rewritten into a query or executed.

use crate::Result;
use crate::context::{Context, Factory};
use crate::error::guard;
use crate::function::{BindInfo, FunctionDocs, FunctionSignature};
use crate::opaque::Opaque;
use crate::sys::*;
use crate::text::str_arg;

/// Rewrites the pragma into a SQL query to run in its place.
pub trait PragmaQuery {
    fn query(bind: &BindInfo, info: &PragmaQueryInfo, context: &Context) -> Result<()>;
}
/// Executes the pragma for its effect.
pub trait PragmaExec {
    fn exec(bind: &BindInfo, context: &Context) -> Result<()>;
}

bind_trampoline!(query_trampoline, PragmaQuery::query, PragmaQueryInfo, duckdb_v2_pragma_function_query_info_handle);

unsafe extern "C" fn exec_trampoline<F: PragmaExec>(
    bind: duckdb_v2_function_bind_info_handle,
    context: duckdb_v2_context_handle,
    err: *mut duckdb_v2_error_info_handle,
) {
    // SAFETY(ffi L8.I1): the engine lends live handles for the duration of the callback; the wrappers are borrowed for
    // the call.
    guard(err, || unsafe { F::exec(&BindInfo::from_raw(bind), &Context::from_raw(context)) });
}

/// A pragma function under construction (owned until registered).
pub struct PragmaFunction {
    raw: duckdb_v2_pragma_function_handle,
}

impl PragmaFunction {
    pub fn new(factory: Factory) -> Result<PragmaFunction> {
        let mut raw = std::ptr::null_mut();
        api_call!(duckdb_v2_pragma_function_create(factory.as_raw(), &mut raw));
        Ok(PragmaFunction { raw })
    }

    pub fn as_raw(&self) -> duckdb_v2_pragma_function_handle {
        self.raw
    }

    pub fn set_name(&self, name: &str) -> Result<&Self> {
        let name = str_arg(name);
        api_call!(duckdb_v2_pragma_function_set_name(self.raw, &name));
        Ok(self)
    }

    pub fn signature(&self) -> Result<FunctionSignature> {
        Ok(api_call_out!(duckdb_v2_pragma_function_get_signature(self.raw) -> FunctionSignature::from_raw))
    }

    pub fn docs(&self) -> Result<FunctionDocs> {
        Ok(api_call_out!(duckdb_v2_pragma_function_get_docs(self.raw) -> FunctionDocs::from_raw))
    }

    pub fn set_user_data<T: 'static>(&self, data: T) -> Result<&Self> {
        let mut opaque = Opaque::new(data);
        api_call!(duckdb_v2_pragma_function_set_user_data(self.raw, &mut opaque));
        Ok(self)
    }

    pub fn set_query<F: PragmaQuery>(&self) -> Result<&Self> {
        api_call!(duckdb_v2_pragma_function_set_query_callback(self.raw, Some(query_trampoline::<F>)));
        Ok(self)
    }

    pub fn set_exec<F: PragmaExec>(&self) -> Result<&Self> {
        api_call!(duckdb_v2_pragma_function_set_exec_callback(self.raw, Some(exec_trampoline::<F>)));
        Ok(self)
    }
}

impl Drop for PragmaFunction {
    fn drop(&mut self) {
        api_destroy!(duckdb_v2_pragma_function_destroy(&mut self.raw));
    }
}

/// The query callback's info.
#[derive(Clone, Copy)]
pub struct PragmaQueryInfo {
    raw: duckdb_v2_pragma_function_query_info_handle,
}

impl PragmaQueryInfo {
    /// The SQL to run in place of the pragma.
    pub fn set_query(&self, query: &str) -> Result<()> {
        let query = str_arg(query);
        api_call!(duckdb_v2_pragma_function_query_set_query(self.raw, &query));
        Ok(())
    }
}
