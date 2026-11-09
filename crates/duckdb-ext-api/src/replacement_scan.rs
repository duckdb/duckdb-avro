//! Replacement scans: resolve a table name the catalog does not have into a table function call, a
//! subquery or an in-memory collection (`SELECT * FROM 'file.ext'`).

use crate::Result;
use crate::context::{Context, Factory};
use crate::opaque::Opaque;
use crate::qname::QualifiedName;
use crate::sys::*;
use crate::text::str_arg;
use crate::value::Value;

/// Decides what a missing table name stands for. Leaving the info untouched declines.
pub trait ReplacementScanCallback {
    fn replace(info: &ReplacementScanInfo, context: &Context) -> Result<()>;
}

trampoline!(
    replace_trampoline,
    ReplacementScanCallback::replace,
    ReplacementScanInfo,
    duckdb_v2_replacement_scan_info_handle
);

/// A replacement scan under construction (owned until registered).
pub struct ReplacementScan {
    raw: duckdb_v2_replacement_scan_handle,
}

impl ReplacementScan {
    pub fn new(factory: Factory) -> Result<ReplacementScan> {
        let mut raw = std::ptr::null_mut();
        api_call!(duckdb_v2_replacement_scan_create(factory.as_raw(), &mut raw));
        Ok(ReplacementScan { raw })
    }

    pub fn as_raw(&self) -> duckdb_v2_replacement_scan_handle {
        self.raw
    }

    pub fn set_user_data<T: 'static>(&self, data: T) -> Result<&Self> {
        let mut opaque = Opaque::new(data);
        api_call!(duckdb_v2_replacement_scan_set_user_data(self.raw, &mut opaque));
        Ok(self)
    }

    pub fn set_callback<F: ReplacementScanCallback>(&self) -> Result<&Self> {
        api_call!(duckdb_v2_replacement_scan_set_callback(self.raw, Some(replace_trampoline::<F>)));
        Ok(self)
    }
}

impl Drop for ReplacementScan {
    fn drop(&mut self) {
        api_destroy!(duckdb_v2_replacement_scan_destroy(&mut self.raw));
    }
}

/// The callback's info: the missing name, and the replacement to build.
#[derive(Clone, Copy)]
pub struct ReplacementScanInfo {
    raw: duckdb_v2_replacement_scan_info_handle,
}

impl ReplacementScanInfo {
    /// The name the query referenced.
    pub fn name(&self) -> Result<QualifiedName> {
        Ok(api_call_out!(duckdb_v2_replacement_scan_get_name(self.raw) -> QualifiedName::from_raw))
    }

    opaque_getter!(user_data, duckdb_v2_replacement_scan_get_user_data);

    /// Replaces the name with a call of the table function `name`.
    pub fn set_function_name(&self, name: &QualifiedName) -> Result<&Self> {
        api_call!(duckdb_v2_replacement_scan_set_function_name(self.raw, name.as_raw()));
        Ok(self)
    }

    /// A positional argument of the function call.
    pub fn add_argument(&self, value: &Value) -> Result<&Self> {
        api_call!(duckdb_v2_replacement_scan_add_argument(self.raw, value.as_raw()));
        Ok(self)
    }

    /// A named argument of the function call.
    pub fn add_named_argument(&self, name: &str, value: &Value) -> Result<&Self> {
        let name = str_arg(name);
        api_call!(duckdb_v2_replacement_scan_add_named_argument(self.raw, &name, value.as_raw()));
        Ok(self)
    }

    /// Replaces the name with a subquery.
    pub fn set_subquery(&self, sql: &str) -> Result<&Self> {
        let sql = str_arg(sql);
        api_call!(duckdb_v2_replacement_scan_set_subquery(self.raw, &sql));
        Ok(self)
    }

    /// Replaces the name with a scan of a column data collection whose columns are `names`.
    #[expect(
        clippy::not_unsafe_ptr_arg_deref,
        reason = "the collection handle is passed through to the engine, not dereferenced here"
    )]
    pub fn set_collection(&self, collection: duckdb_v2_column_data_collection_handle, names: &[&str]) -> Result<&Self> {
        let names: Vec<duckdb_v2_identifier_t> = names.iter().map(|n| str_arg(n)).collect();
        api_call!(duckdb_v2_replacement_scan_set_collection(
            self.raw,
            collection,
            names.as_ptr(),
            names.len() as idx_t
        ));
        Ok(self)
    }

    /// The alias the replacement is known under.
    pub fn set_alias(&self, alias: &str) -> Result<&Self> {
        let alias = str_arg(alias);
        api_call!(duckdb_v2_replacement_scan_set_alias(self.raw, &alias));
        Ok(self)
    }
}
