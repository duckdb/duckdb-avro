//! Errors: the engine's `DUCKDB_V2_ERROR` code plus its rendered text, and the error-slot protocol of
//! callbacks (a callback fills the live `duckdb_v2_error_info_handle` the engine lends it).

use std::fmt;

use crate::sys::*;
use crate::text::{str_arg, str_to_string};

/// An error reported by the engine, or raised by extension code to be reported to the engine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Error {
    /// The `DUCKDB_V2_ERROR_*` code.
    pub code: DUCKDB_V2_ERROR,
    /// The message body without the `"<Type> Error: "` prefix (`ErrorData::RawMessage()`): the engine prepends the
    /// prefix of the code when the error is reported back to it ([`Error::write_to`]), so an error read from a failed
    /// call and re-reported renders the prefix exactly once. A message the engine rendered without a raw body (an
    /// error an extension set directly) is kept as it is.
    pub message: String,
    /// `ErrorData::ExtraInfo()`: the named values of an error read from a failed call (the `position` and
    /// `error_subtype` of a parser error, the `candidates` of a catalog lookup, ...), in the engine's order
    /// (`duckdb_v2_error_info_get_extra_info`). Empty for an error raised in Rust.
    pub extra_info: Vec<(String, String)>,
    /// `ErrorData::Message()` exactly as the engine rendered it, for an error read from a failed call
    /// (`duckdb_v2_error_info_get_text`): the prefix plus the body, plus whatever the engine appended (the query
    /// location of a parser error, the assertion notice of an internal one). `None` for an error raised in Rust.
    pub rendered: Option<String>,
}

pub type Result<T> = std::result::Result<T, Error>;

impl Error {
    pub fn new(code: DUCKDB_V2_ERROR, message: impl Into<String>) -> Error {
        Error { code, message: message.into(), extra_info: Vec::new(), rendered: None }
    }

    /// `InvalidInputException`.
    pub fn invalid_input(message: impl Into<String>) -> Error {
        Error::new(DUCKDB_V2_ERROR_INPUT_INVALID, message)
    }

    /// `IOException`.
    pub fn io(message: impl Into<String>) -> Error {
        Error::new(DUCKDB_V2_ERROR_IO_GENERAL, message)
    }

    /// `BinderException`.
    pub fn binder(message: impl Into<String>) -> Error {
        Error::new(DUCKDB_V2_ERROR_QUERY_BINDER, message)
    }

    /// `ConversionException`.
    pub fn conversion(message: impl Into<String>) -> Error {
        Error::new(DUCKDB_V2_ERROR_TYPE_CONVERSION, message)
    }

    /// `NotImplementedException`.
    pub fn not_implemented(message: impl Into<String>) -> Error {
        Error::new(DUCKDB_V2_ERROR_QUERY_NOT_IMPLEMENTED, message)
    }

    /// `InternalException`.
    pub fn internal(message: impl Into<String>) -> Error {
        Error::new(DUCKDB_V2_ERROR_RUNTIME_INTERNAL, message)
    }

    /// `OutOfRangeException`.
    pub fn out_of_range(message: impl Into<String>) -> Error {
        Error::new(DUCKDB_V2_ERROR_INPUT_OUT_OF_RANGE, message)
    }

    /// `ParserException`.
    pub fn parser(message: impl Into<String>) -> Error {
        Error::new(DUCKDB_V2_ERROR_QUERY_PARSER, message)
    }

    /// `InvalidTypeException`.
    pub fn invalid_type(message: impl Into<String>) -> Error {
        Error::new(DUCKDB_V2_ERROR_TYPE_INVALID, message)
    }

    /// `SerializationException`.
    pub fn serialization(message: impl Into<String>) -> Error {
        Error::new(DUCKDB_V2_ERROR_DATABASE_SERIALIZATION, message)
    }

    /// `InvalidConfigurationException`.
    pub fn invalid_configuration(message: impl Into<String>) -> Error {
        Error::new(DUCKDB_V2_ERROR_CONFIGURATION_INVALID, message)
    }

    /// `PermissionException`.
    pub fn permission(message: impl Into<String>) -> Error {
        Error::new(DUCKDB_V2_ERROR_CONFIGURATION_PERMISSION, message)
    }

    /// `OutOfMemoryException`.
    pub fn out_of_memory(message: impl Into<String>) -> Error {
        Error::new(DUCKDB_V2_ERROR_RESOURCE_OUT_OF_MEMORY, message)
    }

    /// `ConstraintException`.
    pub fn constraint(message: impl Into<String>) -> Error {
        Error::new(DUCKDB_V2_ERROR_DATABASE_CONSTRAINT, message)
    }

    /// `CatalogException`.
    pub fn catalog(message: impl Into<String>) -> Error {
        Error::new(DUCKDB_V2_ERROR_DATABASE_CATALOG, message)
    }

    /// `ExecutorException`.
    pub fn executor(message: impl Into<String>) -> Error {
        Error::new(DUCKDB_V2_ERROR_QUERY_EXECUTOR, message)
    }

    /// `ParameterNotResolvedException`: the engine's "parameter types could not be resolved yet" signal, which bind
    /// callbacks must propagate untouched.
    pub fn parameter_not_resolved(message: impl Into<String>) -> Error {
        Error::new(DUCKDB_V2_ERROR_QUERY_PARAMETER_NOT_RESOLVED, message)
    }

    /// Whether this is the engine's "parameter types could not be resolved yet" signal.
    pub fn is_parameter_not_resolved(&self) -> bool {
        self.code == DUCKDB_V2_ERROR_QUERY_PARAMETER_NOT_RESOLVED
    }

    /// Whether a result's schema / result type is not known yet (`DUCKDB_V2_ERROR_INPUT_METADATA_DEFERRED`: an
    /// expanding statement the engine binds while it runs; ask again after the first chunk).
    pub fn is_metadata_deferred(&self) -> bool {
        self.code == DUCKDB_V2_ERROR_INPUT_METADATA_DEFERRED
    }

    /// Whether the running statement was interrupted (`DUCKDB_V2_ERROR_RUNTIME_INTERRUPT`).
    pub fn is_interrupt(&self) -> bool {
        self.code == DUCKDB_V2_ERROR_RUNTIME_INTERRUPT
    }

    /// Whether the entry point reports `DUCKDB_V2_ERROR_QUERY_NOT_IMPLEMENTED`.
    pub fn is_not_implemented(&self) -> bool {
        self.code == DUCKDB_V2_ERROR_QUERY_NOT_IMPLEMENTED
    }

    /// The engine's table lacks an entry this crate needs (an engine older than the header).
    #[cold]
    #[inline(never)]
    pub fn missing_entry(name: &str) -> Error {
        Error::new(
            DUCKDB_V2_ERROR_API,
            format!(
                "the loading DuckDB does not provide the C API function {name} (its API table has no entry for it)"
            ),
        )
    }

    /// `ErrorData::Message()`: the message with its `"<Type> Error: "` prefix, as the engine renders the error
    /// (`"Parser Error: syntax error ..."`) - the engine's own rendering for an error read from a failed call.
    /// A message that already carries a prefix (one the engine rendered without a raw body) is returned as it is.
    pub fn full_message(&self) -> String {
        if let Some(rendered) = &self.rendered {
            return rendered.clone();
        }
        if let Some(pos) = self.message.find(" Error: ")
            && !self.message[..pos].is_empty()
            && self.message[..pos].chars().all(|c| c.is_ascii_alphabetic() || c == ' ')
        {
            return self.message.clone();
        }
        format!("{} Error: {}", error_type_name(self.code), self.message)
    }

    /// Writes this error into an error slot the engine lent to a callback (`duckdb_v2_error_info_set_code`
    /// + `set_text`). A null slot, or a slot without a live info, swallows the error.
    #[expect(
        clippy::not_unsafe_ptr_arg_deref,
        reason = "the error slot the engine lends to a callback: null-checked, then the C API contract (a live error info) applies"
    )]
    pub fn write_to(&self, slot: *mut duckdb_v2_error_info_handle) {
        if slot.is_null() {
            return;
        }
        // SAFETY(ffi): the engine hands callbacks a slot that points at a live error info.
        let info = unsafe { *slot };
        if info.is_null() {
            return;
        }
        let api = crate::api::api();
        if let Some(set_code) = api.duckdb_v2_error_info_set_code {
            // SAFETY(ffi): `info` is the live error info the engine lent to the callback (non-null, checked above).
            unsafe { set_code(info, self.code) };
        }
        if let Some(set_text) = api.duckdb_v2_error_info_set_text {
            let text = str_arg(&self.message);
            // SAFETY(ffi): live info handle; the text outlives the call.
            unsafe { set_text(info, &text) };
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

/// The exception type the engine renders for a `DUCKDB_V2_ERROR` code (`TryGetExceptionTypeFromErrorCode` +
/// `Exception::ExceptionTypeToString`): the word before `" Error: "` in the engine's messages. Codes without a
/// specific type (`DUCKDB_V2_ERROR_API`, unmapped values) render as `ExceptionType::INVALID`.
pub fn error_type_name(code: DUCKDB_V2_ERROR) -> &'static str {
    match code {
        DUCKDB_V2_ERROR_INPUT_INVALID => "Invalid Input",
        DUCKDB_V2_ERROR_INPUT_OUT_OF_RANGE => "Out of Range",
        DUCKDB_V2_ERROR_INPUT_OBJECT_SIZE => "Object Size",
        DUCKDB_V2_ERROR_IO_GENERAL => "IO",
        DUCKDB_V2_ERROR_IO_NETWORK => "Network",
        DUCKDB_V2_ERROR_IO_HTTP => "HTTP",
        DUCKDB_V2_ERROR_RESOURCE_OUT_OF_MEMORY => "Out of Memory",
        DUCKDB_V2_ERROR_RESOURCE_CONNECTION => "Connection",
        DUCKDB_V2_ERROR_RESOURCE_DEPENDENCY => "Dependency",
        DUCKDB_V2_ERROR_RESOURCE_MISSING_EXTENSION => "Missing Extension",
        DUCKDB_V2_ERROR_RESOURCE_AUTOLOAD => "Extension Autoloading",
        DUCKDB_V2_ERROR_RESOURCE_IN_USE => "Resource In Use",
        DUCKDB_V2_ERROR_TYPE_CONVERSION => "Conversion",
        DUCKDB_V2_ERROR_TYPE_UNKNOWN => "Unknown Type",
        DUCKDB_V2_ERROR_TYPE_INVALID => "Invalid type",
        DUCKDB_V2_ERROR_TYPE_MISMATCH => "Mismatch Type",
        DUCKDB_V2_ERROR_TYPE_DECIMAL => "Decimal",
        DUCKDB_V2_ERROR_TYPE_DIVIDE_BY_ZERO => "Divide by Zero",
        DUCKDB_V2_ERROR_QUERY_PARSER => "Parser",
        DUCKDB_V2_ERROR_QUERY_SYNTAX => "Syntax",
        DUCKDB_V2_ERROR_QUERY_BINDER => "Binder",
        DUCKDB_V2_ERROR_QUERY_PLANNER => "Planner",
        DUCKDB_V2_ERROR_QUERY_OPTIMIZER => "Optimizer",
        DUCKDB_V2_ERROR_QUERY_EXPRESSION => "Expression",
        DUCKDB_V2_ERROR_QUERY_EXECUTOR => "Executor",
        DUCKDB_V2_ERROR_QUERY_SCHEDULER => "Scheduler",
        DUCKDB_V2_ERROR_QUERY_NOT_IMPLEMENTED => "Not implemented",
        DUCKDB_V2_ERROR_QUERY_PARAMETER_NOT_RESOLVED => "Parameter Not Resolved",
        DUCKDB_V2_ERROR_QUERY_PARAMETER_NOT_ALLOWED => "Parameter Not Allowed",
        DUCKDB_V2_ERROR_DATABASE_CATALOG => "Catalog",
        DUCKDB_V2_ERROR_DATABASE_TRANSACTION => "TransactionContext",
        DUCKDB_V2_ERROR_DATABASE_CONSTRAINT => "Constraint",
        DUCKDB_V2_ERROR_DATABASE_INDEX => "Index",
        DUCKDB_V2_ERROR_DATABASE_SEQUENCE => "Sequence",
        DUCKDB_V2_ERROR_DATABASE_STATISTICS => "Stat",
        DUCKDB_V2_ERROR_DATABASE_SERIALIZATION => "Serialization",
        DUCKDB_V2_ERROR_CONFIGURATION_SETTINGS => "Settings",
        DUCKDB_V2_ERROR_CONFIGURATION_INVALID => "Invalid Configuration",
        DUCKDB_V2_ERROR_CONFIGURATION_PERMISSION => "Permission",
        DUCKDB_V2_ERROR_RUNTIME_INTERNAL => "INTERNAL",
        DUCKDB_V2_ERROR_RUNTIME_FATAL => "FATAL",
        DUCKDB_V2_ERROR_RUNTIME_INTERRUPT => "INTERRUPT",
        DUCKDB_V2_ERROR_RUNTIME_NULL_POINTER => "NullPointer",
        _ => "Invalid",
    }
}

impl std::error::Error for Error {}

/// The error the engine attached to a failed call, read (and destroyed) from the info the call returned: the raw
/// message body when the engine rendered one (`duckdb_v2_error_info_get_raw_text`), else the full text, and the
/// info's own code when it carries one.
#[cold]
#[inline(never)]
pub(crate) fn take_error(code: DUCKDB_V2_ERROR, mut info: duckdb_v2_error_info_handle) -> Error {
    let mut message = String::new();
    let mut rendered = None;
    let mut extra_info = Vec::new();
    let mut actual = code;
    if !info.is_null() {
        let api = crate::api::api();
        let mut text = duckdb_v2_str::EMPTY;
        if let Some(get_text) = api.duckdb_v2_error_info_get_text
            // SAFETY(ffi): live info handle returned by the failing call.
            && unsafe { get_text(info, &mut text) } == DUCKDB_V2_ERROR_NONE
        {
            let full = str_to_string(text);
            if !full.is_empty() {
                rendered = Some(full);
            }
        }
        if let Some(get_raw_text) = api.duckdb_v2_error_info_get_raw_text
            // SAFETY(ffi): `info` is the live error info handle returned by the failing call; `text` is a local
            // out-parameter.
            && unsafe { get_raw_text(info, &mut text) } == DUCKDB_V2_ERROR_NONE
        {
            message = str_to_string(text);
        }
        if message.is_empty()
            && let Some(full) = &rendered
        {
            message = full.clone();
        }
        if let (Some(get_count), Some(get_entry)) =
            (api.duckdb_v2_error_info_get_extra_info_count, api.duckdb_v2_error_info_get_extra_info)
        {
            let mut count = 0;
            // SAFETY(ffi): `info` is the live error info handle returned by the failing call; `count` is a local
            // out-parameter.
            if unsafe { get_count(info, &mut count) } == DUCKDB_V2_ERROR_NONE {
                for index in 0..count {
                    let mut key = duckdb_v2_str::EMPTY;
                    let mut value = duckdb_v2_str::EMPTY;
                    // SAFETY(ffi): `info` is the live error info handle returned by the failing call; `index` is below the
                    // count it reported; `key` / `value` are local out-parameters.
                    if unsafe { get_entry(info, index, &mut key, &mut value) } == DUCKDB_V2_ERROR_NONE {
                        extra_info.push((str_to_string(key), str_to_string(value)));
                    }
                }
            }
        }
        if let Some(get_code) = api.duckdb_v2_error_info_get_code {
            let mut info_code = code;
            // SAFETY(ffi): `info` is the live error info handle returned by the failing call; `info_code` is a local
            // out-parameter.
            if unsafe { get_code(info, &mut info_code) } == DUCKDB_V2_ERROR_NONE && info_code != DUCKDB_V2_ERROR_NONE {
                actual = info_code;
            }
        }
        if let Some(destroy) = api.duckdb_v2_error_info_destroy {
            // SAFETY(ffi): the info is owned by us (returned through the slot of the failing call).
            unsafe { destroy(&mut info) };
        }
    }
    if message.is_empty() {
        message = format!("DuckDB C API error code {}", code.0);
    }
    Error { code: actual, message, extra_info, rendered }
}

/// The message of an error info an entry point filled as a secondary result (the failed rows of a try-cast), or
/// `""` for a null info; the info is destroyed.
pub(crate) fn take_message(info: duckdb_v2_error_info_handle) -> String {
    if info.is_null() {
        return String::new();
    }
    take_error(DUCKDB_V2_ERROR_TYPE_CONVERSION, info).message
}

/// Converts the outcome of an API call: `DUCKDB_V2_ERROR_NONE` is `Ok`, anything else carries the text of
/// the info the call returned through its slot.
#[inline]
pub fn check(code: DUCKDB_V2_ERROR, info: duckdb_v2_error_info_handle) -> Result<()> {
    if code == DUCKDB_V2_ERROR_NONE {
        if !info.is_null() {
            release_success_info(info);
        }
        return Ok(());
    }
    Err(take_error(code, info))
}

/// Releases an info a call returned together with a success code (rare: the hot path of `check` stays a compare).
#[cold]
#[inline(never)]
fn release_success_info(mut info: duckdb_v2_error_info_handle) {
    if let Some(destroy) = crate::api::api().duckdb_v2_error_info_destroy {
        // SAFETY(ffi): an info returned with a success code is still ours to release.
        unsafe { destroy(&mut info) };
    }
}

/// Runs callback code under the error-slot protocol: an `Err` is written into the slot, and a panic is
/// caught and reported as an internal error, so nothing unwinds into the engine.
#[expect(
    clippy::disallowed_methods,
    reason = "the extension API's FFI boundary: a panic becomes the callback's error instead of unwinding into the engine"
)]
pub fn guard(slot: *mut duckdb_v2_error_info_handle, f: impl FnOnce() -> Result<()>) {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)) {
        Ok(Ok(())) => {}
        Ok(Err(error)) => error.write_to(slot),
        Err(payload) => {
            let text = payload
                .downcast_ref::<&str>()
                .map(|s| s.to_string())
                .or_else(|| payload.downcast_ref::<String>().cloned())
                .unwrap_or_else(|| "unknown panic".to_string());
            Error::internal(format!("INTERNAL Error: extension callback panicked: {text}")).write_to(slot);
        }
    }
}
