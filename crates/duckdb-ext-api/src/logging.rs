//! Log sinks: where the engine's log entries go when the sink is selected.

use std::ffi::c_void;

use crate::Result;
use crate::context::{Factory, LogLevel};
use crate::error::guard;
use crate::opaque::Opaque;
use crate::sys::*;
use crate::text::{str_arg, str_to_string};

/// `DUCKDB_V2_LOG_SCOPE`.
pub type LogScope = DUCKDB_V2_LOG_SCOPE;

/// One log entry.
#[derive(Debug, Clone)]
pub struct LogEntry {
    pub timestamp: i64,
    pub level: LogLevel,
    pub scope: LogScope,
    pub log_type: String,
    pub message: String,
}

/// Receives log entries.
pub trait LogSinkWrite {
    fn write(info: &LogSinkWriteInfo, user_data: *mut c_void, entry: &LogEntry) -> Result<()>;
}
/// Flushes buffered entries.
pub trait LogSinkFlush {
    fn flush(user_data: *mut c_void) -> Result<()>;
}

unsafe extern "C" fn write_trampoline<F: LogSinkWrite>(
    info: duckdb_v2_log_sink_write_info_handle,
    user_data: *mut c_void,
    timestamp: i64,
    level: DUCKDB_V2_LOG_LEVEL,
    scope: DUCKDB_V2_LOG_SCOPE,
    log_type: *const duckdb_v2_str,
    message: *const duckdb_v2_str,
    err: *mut duckdb_v2_error_info_handle,
) {
    guard(err, || {
        let entry = LogEntry {
            timestamp,
            level,
            scope,
            // SAFETY(ffi): the engine passes live string views for the duration of the callback.
            log_type: if log_type.is_null() { String::new() } else { str_to_string(unsafe { *log_type }) },
            message: if message.is_null() { String::new() } else { str_to_string(unsafe { *message }) },
        };
        F::write(&LogSinkWriteInfo { raw: info }, user_data, &entry)
    });
}

unsafe extern "C" fn flush_trampoline<F: LogSinkFlush>(
    _info: duckdb_v2_log_sink_flush_info_handle,
    user_data: *mut c_void,
    err: *mut duckdb_v2_error_info_handle,
) {
    guard(err, || F::flush(user_data));
}

/// A log sink under construction (owned until registered).
pub struct LogSink {
    raw: duckdb_v2_log_sink_handle,
}

impl LogSink {
    pub fn new(factory: Factory) -> Result<LogSink> {
        let mut raw: duckdb_v2_log_sink_handle = std::ptr::null_mut();
        api_call!(duckdb_v2_log_sink_create(factory.as_raw(), &mut raw));
        Ok(LogSink { raw })
    }

    pub fn as_raw(&self) -> duckdb_v2_log_sink_handle {
        self.raw
    }

    /// The name `SET logging_storage = '<name>'` selects.
    pub fn set_name(&self, name: &str) -> Result<&Self> {
        let name = str_arg(name);
        api_call!(duckdb_v2_log_sink_set_name(self.raw, &name));
        Ok(self)
    }

    pub fn set_user_data<T: 'static>(&self, data: T) -> Result<&Self> {
        let mut opaque = Opaque::new(data);
        api_call!(duckdb_v2_log_sink_set_user_data(self.raw, &mut opaque));
        Ok(self)
    }

    pub fn set_write<F: LogSinkWrite>(&self) -> Result<&Self> {
        api_call!(duckdb_v2_log_sink_set_write_callback(self.raw, Some(write_trampoline::<F>)));
        Ok(self)
    }

    pub fn set_flush<F: LogSinkFlush>(&self) -> Result<&Self> {
        api_call!(duckdb_v2_log_sink_set_flush_callback(self.raw, Some(flush_trampoline::<F>)));
        Ok(self)
    }
}

impl Drop for LogSink {
    fn drop(&mut self) {
        api_destroy!(duckdb_v2_log_sink_destroy(&mut self.raw));
    }
}

/// The write callback's info: the ids of where the entry came from.
#[derive(Clone, Copy)]
pub struct LogSinkWriteInfo {
    raw: duckdb_v2_log_sink_write_info_handle,
}

macro_rules! optional_id {
    ($(#[$doc:meta])* $name:ident, $api:ident) => {
        $(#[$doc])*
        pub fn $name(&self) -> Result<Option<u64>> {
            let mut id: idx_t = 0;
            let mut set = false;
            api_call!($api(self.raw, &mut id, &mut set));
            Ok(if set { Some(id) } else { None })
        }
    };
}

impl LogSinkWriteInfo {
    optional_id!(/// The connection id (connection and query scope).
        connection_id, duckdb_v2_log_sink_write_info_get_connection_id);
    optional_id!(/// The transaction id.
        transaction_id, duckdb_v2_log_sink_write_info_get_transaction_id);
    optional_id!(/// The query id (query scope).
        query_id, duckdb_v2_log_sink_write_info_get_query_id);
    optional_id!(/// The thread id.
        thread_id, duckdb_v2_log_sink_write_info_get_thread_id);
}
