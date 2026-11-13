//! The extension being loaded (`duckdb_v2_extension_handle`): where catalog entries are registered.
//! Everything registered here is attributed to the extension (`duckdb_functions().extension_name`,
//! the extension's schema when it uses one) and lives as long as the database.

use crate::Result;
use crate::aggregate::AggregateFunction;
use crate::cast::CastFunction;
use crate::context::{Connection, Instance};
use crate::copy::CopyFunction;
use crate::custom::{CustomOption, CustomType};
use crate::custom_file_system::CustomFileSystemBuilder;
use crate::logging::LogSink;
use crate::macro_function::MacroFunction;
use crate::pragma::PragmaFunction;
use crate::replacement_scan::ReplacementScan;
use crate::scalar::ScalarFunction;
use crate::sys::*;
use crate::table::{MultiFileFunction, TableFunction};
use crate::text::str_arg;

/// The extension under load (borrowed for the duration of the entrypoint).
#[derive(Clone, Copy)]
pub struct Extension {
    raw: duckdb_v2_extension_handle,
}

impl Extension {
    /// # Safety
    /// (ffi L8.I1) `raw` must be a handle the C API returned for this type and that is still live. An owned wrapper
    /// destroys the handle on drop, so nothing else may destroy it; a borrowed (`Copy`) wrapper must not be used past
    /// the call that lent the handle.
    pub unsafe fn from_raw(raw: duckdb_v2_extension_handle) -> Extension {
        Extension { raw }
    }

    pub fn as_raw(&self) -> duckdb_v2_extension_handle {
        self.raw
    }

    pub fn register_scalar_function(&self, function: &ScalarFunction) -> Result<()> {
        api_call!(duckdb_v2_extension_register_scalar_function(self.raw, function.as_raw()));
        Ok(())
    }

    pub fn register_aggregate_function(&self, function: &AggregateFunction) -> Result<()> {
        api_call!(duckdb_v2_extension_register_aggregate_function(self.raw, function.as_raw()));
        Ok(())
    }

    /// `ExtensionLoader::RegisterCollation(CreateCollationInfo(name, function, combinable, not_required_for_equality))`:
    /// a collation backed by a sort-key function (one VARCHAR parameter, a BLOB or VARCHAR key), which is registered
    /// under its own name as well. newer engines only: an error for a library without the entry.
    pub fn register_collation(
        &self,
        name: &str,
        function: &ScalarFunction,
        combinable: bool,
        not_required_for_equality: bool,
    ) -> Result<()> {
        let name = str_arg(name);
        api_call!(duckdb_v2_extension_register_collation(
            self.raw,
            &name,
            function.as_raw(),
            combinable,
            not_required_for_equality
        ));
        Ok(())
    }

    pub fn register_table_function(&self, function: &TableFunction) -> Result<()> {
        api_call!(duckdb_v2_extension_register_table_function(self.raw, function.as_raw()));
        Ok(())
    }

    /// Registers the multi-file wrapper of a single-file table function registered before.
    pub fn register_multi_file_function(&self, function: &MultiFileFunction) -> Result<()> {
        api_call!(duckdb_v2_extension_register_multi_file_function(self.raw, function.as_raw()));
        Ok(())
    }

    pub fn register_copy_function(&self, function: &CopyFunction) -> Result<()> {
        api_call!(duckdb_v2_extension_register_copy_function(self.raw, function.as_raw()));
        Ok(())
    }

    pub fn register_cast_function(&self, function: &CastFunction) -> Result<()> {
        api_call!(duckdb_v2_extension_register_cast_function(self.raw, function.as_raw()));
        Ok(())
    }

    pub fn register_pragma_function(&self, function: &PragmaFunction) -> Result<()> {
        api_call!(duckdb_v2_extension_register_pragma_function(self.raw, function.as_raw()));
        Ok(())
    }

    /// Macros can only be registered by an extension (not through a connection).
    pub fn register_macro_function(&self, function: &MacroFunction) -> Result<()> {
        api_call!(duckdb_v2_extension_register_macro_function(self.raw, function.as_raw()));
        Ok(())
    }

    pub fn register_custom_type(&self, type_: &CustomType) -> Result<()> {
        api_call!(duckdb_v2_extension_register_custom_type(self.raw, type_.as_raw()));
        Ok(())
    }

    pub fn register_custom_option(&self, option: &CustomOption) -> Result<()> {
        api_call!(duckdb_v2_extension_register_custom_option(self.raw, option.as_raw()));
        Ok(())
    }

    pub fn register_replacement_scan(&self, scan: &ReplacementScan) -> Result<()> {
        api_call!(duckdb_v2_extension_register_replacement_scan(self.raw, scan.as_raw()));
        Ok(())
    }

    pub fn register_log_sink(&self, sink: &LogSink) -> Result<()> {
        api_call!(duckdb_v2_extension_register_log_sink(self.raw, sink.as_raw()));
        Ok(())
    }

    /// `VirtualFileSystem::RegisterSubSystem`: paths the file system's `can_handle` accepts are routed to it from
    /// then on. newer engines only: an error for a library without the entry.
    pub fn register_file_system(&self, file_system: &CustomFileSystemBuilder) -> Result<()> {
        api_call!(duckdb_v2_extension_register_file_system(self.raw, file_system.as_raw()));
        Ok(())
    }
}

/// The connection-level registrations (what a client program registers without being an extension).
impl Connection {
    pub fn register_scalar_function(&self, function: &ScalarFunction) -> Result<()> {
        api_call!(duckdb_v2_connection_register_scalar_function(self.as_raw(), function.as_raw()));
        Ok(())
    }

    pub fn register_aggregate_function(&self, function: &AggregateFunction) -> Result<()> {
        api_call!(duckdb_v2_connection_register_aggregate_function(self.as_raw(), function.as_raw()));
        Ok(())
    }

    pub fn register_table_function(&self, function: &TableFunction) -> Result<()> {
        api_call!(duckdb_v2_connection_register_table_function(self.as_raw(), function.as_raw()));
        Ok(())
    }

    pub fn register_multi_file_function(&self, function: &MultiFileFunction) -> Result<()> {
        api_call!(duckdb_v2_connection_register_multi_file_function(self.as_raw(), function.as_raw()));
        Ok(())
    }

    pub fn register_copy_function(&self, function: &CopyFunction) -> Result<()> {
        api_call!(duckdb_v2_connection_register_copy_function(self.as_raw(), function.as_raw()));
        Ok(())
    }

    pub fn register_cast_function(&self, function: &CastFunction) -> Result<()> {
        api_call!(duckdb_v2_connection_register_cast_function(self.as_raw(), function.as_raw()));
        Ok(())
    }

    pub fn register_pragma_function(&self, function: &PragmaFunction) -> Result<()> {
        api_call!(duckdb_v2_connection_register_pragma_function(self.as_raw(), function.as_raw()));
        Ok(())
    }

    pub fn register_custom_type(&self, type_: &CustomType) -> Result<()> {
        api_call!(duckdb_v2_connection_register_custom_type(self.as_raw(), type_.as_raw()));
        Ok(())
    }

    pub fn register_custom_option(&self, option: &CustomOption) -> Result<()> {
        api_call!(duckdb_v2_connection_register_custom_option(self.as_raw(), option.as_raw()));
        Ok(())
    }

    pub fn register_replacement_scan(&self, scan: &ReplacementScan) -> Result<()> {
        api_call!(duckdb_v2_connection_register_replacement_scan(self.as_raw(), scan.as_raw()));
        Ok(())
    }
}

impl Instance {
    pub fn register_replacement_scan(&self, scan: &ReplacementScan) -> Result<()> {
        api_call!(duckdb_v2_instance_register_replacement_scan(self.as_raw(), scan.as_raw()));
        Ok(())
    }

    pub fn register_log_sink(&self, sink: &LogSink) -> Result<()> {
        api_call!(duckdb_v2_instance_register_log_sink(self.as_raw(), sink.as_raw()));
        Ok(())
    }
}

/// What a binary that links an extension crate statically registers: the name, the version and the
/// `<name>_init_c_api_v2` entrypoint, handed to the engine's static-extension registration.
#[derive(Clone, Copy)]
pub struct LinkedEntrypoint {
    pub name: &'static str,
    pub version: &'static str,
    pub init: unsafe extern "C" fn(*mut duckdb_v2_extension_input),
}
