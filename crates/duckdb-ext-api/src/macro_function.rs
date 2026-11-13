//! Macro functions: scalar and table macros defined by a SQL body over their parameters.

use crate::Result;
use crate::context::Factory;
use crate::function::{FunctionDocs, FunctionSignature};
use crate::sys::*;
use crate::text::str_arg;

/// A macro function under construction (owned until registered).
pub struct MacroFunction {
    raw: duckdb_v2_macro_function_handle,
}

impl MacroFunction {
    fn create(factory: Factory, kind: DUCKDB_V2_MACRO_FUNCTION_TYPE) -> Result<MacroFunction> {
        let mut raw = std::ptr::null_mut();
        api_call!(duckdb_v2_macro_function_create(factory.as_raw(), kind, &mut raw));
        Ok(MacroFunction { raw })
    }

    /// A scalar macro: the body is a single expression.
    pub fn scalar(factory: Factory) -> Result<MacroFunction> {
        MacroFunction::create(factory, DUCKDB_V2_MACRO_FUNCTION_TYPE_SCALAR)
    }

    /// A table macro: the body is a single SELECT query.
    pub fn table(factory: Factory) -> Result<MacroFunction> {
        MacroFunction::create(factory, DUCKDB_V2_MACRO_FUNCTION_TYPE_TABLE)
    }

    pub fn as_raw(&self) -> duckdb_v2_macro_function_handle {
        self.raw
    }

    pub fn set_name(&self, name: &str) -> Result<&Self> {
        let name = str_arg(name);
        api_call!(duckdb_v2_macro_function_set_name(self.raw, &name));
        Ok(self)
    }

    pub fn set_body(&self, body: &str) -> Result<&Self> {
        let body = str_arg(body);
        api_call!(duckdb_v2_macro_function_set_body(self.raw, &body));
        Ok(self)
    }

    pub fn signature(&self) -> Result<FunctionSignature> {
        Ok(api_call_out!(duckdb_v2_macro_function_get_signature(self.raw) -> FunctionSignature::from_raw))
    }

    pub fn docs(&self) -> Result<FunctionDocs> {
        Ok(api_call_out!(duckdb_v2_macro_function_get_docs(self.raw) -> FunctionDocs::from_raw))
    }
}

impl Drop for MacroFunction {
    fn drop(&mut self) {
        api_destroy!(duckdb_v2_macro_function_destroy(&mut self.raw));
    }
}
