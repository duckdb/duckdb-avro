//! Safe Rust bindings for writing DuckDB extensions against the C API v2 (`duckdb_extension_v2.h`).
//!
//! An extension crate built on this library never links the engine: it depends on
//! [`duckdb_capi_sys`] (the C types, standard library only) and on nothing else, and every call goes
//! through the `duckdb_ext_api_v2` function table the loading engine hands to the entrypoint. The same
//! crate therefore works on both load paths:
//!
//! - **dynamic**: built as a `cdylib`, renamed to `<name>.duckdb_extension`, given the metadata footer
//!   (`scripts/append_extension_metadata.py`; `C_STRUCT_UNSTABLE` pinned to the engine's version tag, since no
//!   V2 function is stable yet) and loaded with `LOAD '<path>'` by the engine (`-unsigned`), which `dlopen` it and call `<name>_init_c_api_v2`;
//! - **static**: linked into a binary together with the engine and registered through the engine's
//!   static-extension registration (name, version, `<name>_init_c_api_v2`) before the first instance is
//!   created.
//!
//! [`duckdb_extension!`] generates the entrypoint for both paths. The wrappers follow the handle model
//! of the C API: owned handles ([`LogicalType`], [`Value`], [`DataChunk`], the function builders, ...)
//! are destroyed when dropped; borrowed ones ([`Context`], [`Factory`], [`Vector`], the callback infos)
//! are `Copy` views valid for the duration of the call that handed them out. Every fallible call returns
//! [`Result`] with the engine's error code and text ([`Error`]).
//!
//! Callbacks are plain types implementing a trait ([`ScalarExec`], [`TableBind`], ...): the library
//! registers an `extern "C"` trampoline per type, catches panics and converts the returned `Err` into
//! the error slot the engine passed. Per-function state travels through the `user_data` /
//! `bind_data` / `init_data` slots as [`Opaque`] boxes.
//!
//! See the README of this repository for the extension layout.

pub use duckdb_capi_sys as sys;

#[macro_use]
pub mod api;
pub mod error;
mod text;
#[cfg(kani)]
mod verification;

pub mod aggregate;
pub mod cast;
pub mod chunk;
pub mod client;
pub mod collection;
pub mod context;
pub mod copy;
pub mod custom;
pub mod custom_file_system;
pub mod dynamic;
pub mod expression;
pub mod extension;
pub mod file_system;
pub mod function;
pub mod http;
pub mod logging;
pub mod macro_function;
pub mod metadata;
pub mod object_cache;
pub mod opaque;
pub mod pragma;
pub mod qname;
pub mod query;
pub mod replacement_scan;
pub mod scalar;
pub mod secret;
pub mod statistics;
pub mod sync;
pub mod table;
pub mod types;
pub mod value;
pub mod vector;

pub use aggregate::{
    AggregateBind, AggregateBindInfo, AggregateCombine, AggregateCombineInfo, AggregateDestroy, AggregateDestroyInfo,
    AggregateFinalize, AggregateFinalizeInfo, AggregateFunction, AggregateInit, AggregateInitInfo, AggregateSize,
    AggregateSizeInfo, AggregateStats, AggregateStatsInfo, AggregateUpdate, AggregateUpdateInfo, StateAggregate,
    States,
};
pub use api::{
    api, api_version_string, init_api, init_api_with_version, install_table, is_api_initialized, library_version,
    table_from_symbols,
};
pub use cast::{CastBind, CastBindInfo, CastExec, CastExecInfo, CastFunction, CastMode};
pub use chunk::DataChunk;
pub use client::{AttachOptions, Database, Environment, Library, QueryProgress, SharedConnection};
pub use collection::{AppendState, ColumnDataCollection, SharedScanState, WorkerScanState};
pub use context::{
    AllocatedData, BufferAllocator, Config, Connection, Context, CrsType, Factory, Instance, LogLevel, ParsedCrs,
    SettingScope, parse_crs,
};
pub use copy::{
    CopyFromBind, CopyFromBindInfo, CopyFromExec, CopyFromExecInfo, CopyFromInitGlobal, CopyFromInitGlobalInfo,
    CopyFromInitLocal, CopyFromInitLocalInfo, CopyFromProgress, CopyFromProgressInfo, CopyFunction, CopyToBatch,
    CopyToBatchInfo, CopyToBatchSize, CopyToBatchSizeInfo, CopyToBind, CopyToBindInfo, CopyToFileSize,
    CopyToFileSizeInfo, CopyToFinalize, CopyToFinalizeInfo, CopyToFlush, CopyToFlushInfo, CopyToInit, CopyToInitInfo,
    CopyToPropagateStatistics, CopyToPropagateStatisticsInfo, CopyToSelect, CopyToSelectInfo, CopyToStatistics,
    CopyToStatisticsInfo, CopyToType,
};
pub use custom::{CustomOption, CustomOptionUpdate, CustomOptionUpdateInfo, CustomType};
pub use custom_file_system::{
    CustomFile, CustomFileSystem, CustomFileSystemBuilder, FileListSink, FileOpenFlagsMask, FileOpener, FileWriteMode,
    Logger, NetworkThroughput, OpenRequest,
};
pub use error::{Error, Result};
pub use expression::{Expression, ExpressionType, FilterPropagateResult, OwnedExpression};
pub use extension::{Extension, LinkedEntrypoint};
pub use file_system::{AsyncFileWriter, File, FileFlag, FileOpenOptions, FileSystem};
pub use function::{ArgCounts, BindInfo, FunctionDocs, FunctionSignature, ParameterKind, PropertyKey, PropertyValue};
pub use http::{
    HttpClient, HttpMethod, HttpParam, HttpParams, HttpProvider, HttpProviderImpl, HttpRequest, HttpResponse,
    HttpSession, HttpTiming, OwnedHttpParams, OwnedHttpRequest, OwnedHttpResponse, TransportReusePolicy,
};
pub use logging::{LogSink, LogSinkFlush, LogSinkWrite, LogSinkWriteInfo};
pub use macro_function::MacroFunction;
pub use metadata::{AbiType, ExtensionMetadata};
pub use object_cache::ObjectCacheEntry;
pub use opaque::Opaque;
pub use pragma::{PragmaExec, PragmaFunction, PragmaQuery, PragmaQueryInfo};
pub use qname::QualifiedName;
pub use query::{
    ArrowBatch, ArrowResult, PreparedStatement, QueryResult, ResultType, SerializeFlags, Statement, StatementIterator,
    StatementType, StepStatus,
};
pub use replacement_scan::{ReplacementScan, ReplacementScanCallback, ReplacementScanInfo};
pub use scalar::{
    Monotonicity, ScalarBind, ScalarBindInfo, ScalarExec, ScalarExecInfo, ScalarFunction, ScalarInit, ScalarInitInfo,
    ScalarResolveTypes, ScalarResolveTypesInfo, ScalarStats, ScalarStatsInfo,
};
pub use secret::{
    CreateSecret, CreateSecretInfo, OwnedCreateSecretInfo, RegisteredState, Secret, SecretFunction, SecretPersistType,
    parse_memory_limit,
};
pub use statistics::{Statistics, Stats};
pub use table::{
    AdaptiveFilter, AdaptiveFilterEntry, ExtractField, MultiFileFunction, OperatorResult, OrderPreservation, TableBind,
    TableBindInfo, TableClaimBatch, TableClaimBatchInfo, TableExec, TableExecInfo, TableFilter, TableFilterPushdown,
    TableFilterPushdownInfo, TableFunction, TableFunctionReturnType, TableGetBindInfo, TableGetBindInfoInfo,
    TableGetMetrics, TableGetMetricsInfo, TableInOut, TableInOutInfo, TableInitGlobal, TableInitGlobalInfo,
    TableInitLocal, TableInitLocalInfo, TablePartitionColumnStatistics, TablePartitionColumnStatisticsInfo,
    TablePartitionData, TablePartitionDataInfo, TablePartitionInfo, TablePartitionStats, TablePartitionStatsInfo,
    TablePartitioning, TablePartitioningInfo, TablePrepareReadAhead, TablePrepareReadAheadInfo, TableProgress,
    TableProgressInfo, TableProjectionExpressionPushdown, TableProjectionExpressionPushdownInfo,
    TablePushdownExtractInfo, TableScheduleIo, TableScheduleIoInfo, TableStatistics, TableStatisticsInfo, TableStats,
    TableStatsInfo, TableSupportsPushdownExtract,
};
pub use table::{
    MultiFileCombineSchema, MultiFileCombineSchemaInfo, MultiFilePartitionStats, MultiFilePartitionStatsInfo,
    TableBindReplace, TableBindReplaceInfo,
};
pub use text::quote_identifier;
pub use types::{LogicalType, TypeId};
pub use value::Value;
pub use vector::{
    Arena, Dictionary, Element, ListView, StringView, StringWriter, TypedView, ValidityMut, Vector, VectorType,
    VectorView,
};

/// The body of an extension entrypoint: what [`duckdb_extension!`] calls once the API table is in place.
/// `extension` registers catalog entries, `context` reads and runs queries (a transaction is active).
pub type LoadFn = fn(&Extension, &Context) -> Result<()>;

/// Runs an entrypoint body against the raw input of a C API v2 entrypoint: fetches the function table
/// through `get_api` (returning without touching the error slot when the engine refuses the version,
/// as the C `DUCKDB_EXTENSION_API_INIT` does), wraps the handles, and reports an `Err` or a panic through
/// the error slot. [`duckdb_extension!`] expands to a call of this.
///
/// # Safety
/// (ffi L8.I5) `input` is null or points at a `duckdb_v2_extension_input` live for the duration of the call, whose
/// handles (`extension`, `context`, `err`) the loader lends for the entrypoint (L8.I5); nothing built from them
/// outlives it.
pub unsafe fn run_entrypoint(input: *mut sys::duckdb_v2_extension_input, load: LoadFn) {
    if input.is_null() {
        return;
    }
    // SAFETY(ffi L8.I5): the loader passes a live, non-null `duckdb_v2_extension_input` for the duration of the
    // entrypoint.
    let input = unsafe { &*input };
    if !init_api(input) {
        return;
    }
    // SAFETY(ffi L8.I5): the input's handles are lent for the duration of the entrypoint; the wrappers do not outlive
    // it.
    let extension = unsafe { Extension::from_raw(input.extension) };
    // SAFETY(ffi L8.I5): as for `extension`.
    let context = unsafe { Context::from_raw(input.context) };
    error::guard(input.err, || load(&extension, &context));
}

/// Generates the entrypoint of an extension crate.
///
/// ```ignore
/// duckdb_ext_api::duckdb_extension!(demo_capi, demo_capi_init_c_api_v2, "v0.0.1", load);
///
/// fn load(extension: &Extension, context: &Context) -> Result<()> { ... }
/// ```
///
/// declares the exported `extern "C" fn demo_capi_init_c_api_v2(*mut duckdb_v2_extension_input)` the
/// loaders look for (`LOAD` of the cdylib, or the static registration) - the symbol has to be spelled
/// out because a macro cannot build identifiers, and the macro checks at compile time that it is
/// `<name>_init_c_api_v2` - plus `EXTENSION_NAME`, `EXTENSION_VERSION` and `LINKED_ENTRYPOINT:
/// LinkedEntrypoint` for a binary that links the crate statically and registers `LINKED_ENTRYPOINT.name`,
/// `LINKED_ENTRYPOINT.version` and `LINKED_ENTRYPOINT.init` with the engine.
#[macro_export]
macro_rules! duckdb_extension {
    ($name:ident, $entry:ident, $version:expr, $load:expr) => {
        /// The extension's name: the file base of its `.duckdb_extension` and the prefix of its entrypoint.
        pub const EXTENSION_NAME: &str = stringify!($name);
        /// What `duckdb_extensions()` reports as the extension version.
        pub const EXTENSION_VERSION: &str = $version;

        const _: () = assert!(
            $crate::str_eq(stringify!($entry), concat!(stringify!($name), "_init_c_api_v2")),
            "the entrypoint of a C API v2 extension must be named <extension name>_init_c_api_v2"
        );

        /// `<name>_init_c_api_v2`: the C API v2 entrypoint the loaders look for.
        ///
        /// # Safety
        /// Called by the engine's extension loader with a live `duckdb_v2_extension_input` for the duration of the
        /// call (the C API v2 entrypoint contract, L8.I5).
        #[unsafe(no_mangle)]
        pub unsafe extern "C" fn $entry(input: *mut $crate::sys::duckdb_v2_extension_input) {
            // SAFETY(ffi L8.I5): the loader calls the entrypoint with a live input pointer (the C API v2 entrypoint
            // contract).
            unsafe { $crate::run_entrypoint(input, $load) }
        }

        /// The entrypoint as a binary linking this crate registers it.
        pub const LINKED_ENTRYPOINT: $crate::LinkedEntrypoint =
            $crate::LinkedEntrypoint { name: EXTENSION_NAME, version: EXTENSION_VERSION, init: $entry };
    };
}

/// `const` string equality, for the entrypoint-name check of [`duckdb_extension!`].
#[doc(hidden)]
pub const fn str_eq(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    if a.len() != b.len() {
        return false;
    }
    let mut i = 0;
    while i < a.len() {
        if a[i] != b[i] {
            return false;
        }
        i += 1;
    }
    true
}
