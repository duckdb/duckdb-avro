//! Copy functions: `COPY ... TO 'file' (FORMAT name)` writers and `COPY ... FROM` readers.

use std::ffi::c_void;

use crate::Result;
use crate::chunk::DataChunk;
use crate::collection::ColumnDataCollection;
use crate::context::{Context, Factory};
use crate::opaque::{CallbackSlots, Opaque};
use crate::sys::*;
use crate::text::{str_arg, str_to_string};
use crate::types::LogicalType;
use crate::value::Value;

/// Binds a COPY TO: inspects the columns and options, stores bind data.
pub trait CopyToBind {
    fn bind(info: &CopyToBindInfo, context: &Context) -> Result<()>;
}
/// Chooses the number of rows per batch.
pub trait CopyToBatchSize {
    fn batch_size(info: &CopyToBatchSizeInfo, context: &Context) -> Result<()>;
}
/// Opens the output file (stores init data).
pub trait CopyToInit {
    fn init(info: &CopyToInitInfo, context: &Context) -> Result<()>;
}
/// Prepares a batch of rows (in parallel; stores batch data).
pub trait CopyToBatch {
    fn batch(info: &CopyToBatchInfo, context: &Context) -> Result<()>;
}
/// Writes a prepared batch (in order).
pub trait CopyToFlush {
    fn flush(info: &CopyToFlushInfo<'_>, context: &Context) -> Result<()>;
}
/// Closes the output file.
pub trait CopyToFinalize {
    fn finalize(info: &CopyToFinalizeInfo, context: &Context) -> Result<()>;
}
/// Reports what was written (file size, row count, footer size, column statistics).
pub trait CopyToStatistics {
    fn statistics(info: &CopyToStatisticsInfo, context: &Context) -> Result<()>;
}
/// Reports the size of the file written so far (for `FILE_SIZE_BYTES` rotation).
pub trait CopyToFileSize {
    fn file_size(info: &CopyToFileSizeInfo) -> Result<()>;
}
/// Rewrites the types of the columns before they reach the function (casts added by the engine).
pub trait CopyToSelect {
    fn select(info: &CopyToSelectInfo, context: &Context) -> Result<()>;
}
/// Receives what the optimizer knows about the written columns (e.g. which cannot be NULL).
pub trait CopyToPropagateStatistics {
    fn propagate_statistics(info: &CopyToPropagateStatisticsInfo<'_>, context: &Context) -> Result<()>;
}

/// The kind of statement a COPY TO is bound for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CopyToType {
    CopyToFile,
    ExportDatabase,
}
/// Binds a COPY FROM against the target columns.
pub trait CopyFromBind {
    fn bind(info: &CopyFromBindInfo, context: &Context) -> Result<()>;
}
pub trait CopyFromInitGlobal {
    fn init_global(info: &CopyFromInitGlobalInfo, context: &Context) -> Result<()>;
}
pub trait CopyFromInitLocal {
    fn init_local(info: &CopyFromInitLocalInfo, context: &Context) -> Result<()>;
}
/// Produces the next chunk of rows read from the file.
pub trait CopyFromExec {
    fn exec(info: &CopyFromExecInfo<'_>, context: &Context) -> Result<()>;
}
pub trait CopyFromProgress {
    fn progress(info: &CopyFromProgressInfo, context: &Context) -> Result<()>;
}

trampoline!(to_bind_trampoline, CopyToBind::bind, CopyToBindInfo, duckdb_v2_copy_to_bind_info_handle);
trampoline!(
    to_batch_size_trampoline,
    CopyToBatchSize::batch_size,
    CopyToBatchSizeInfo,
    duckdb_v2_copy_to_batch_size_info_handle
);
trampoline!(to_init_trampoline, CopyToInit::init, CopyToInitInfo, duckdb_v2_copy_to_init_info_handle);
trampoline!(to_batch_trampoline, CopyToBatch::batch, CopyToBatchInfo, duckdb_v2_copy_to_batch_info_handle);
trampoline_cb!(to_flush_trampoline, CopyToFlush::flush, CopyToFlushInfo, duckdb_v2_copy_to_flush_info_handle);
trampoline!(
    to_finalize_trampoline,
    CopyToFinalize::finalize,
    CopyToFinalizeInfo,
    duckdb_v2_copy_to_finalize_info_handle
);
trampoline!(
    to_statistics_trampoline,
    CopyToStatistics::statistics,
    CopyToStatisticsInfo,
    duckdb_v2_copy_to_statistics_info_handle
);
trampoline!(to_select_trampoline, CopyToSelect::select, CopyToSelectInfo, duckdb_v2_copy_to_select_info_handle);
trampoline_cb!(
    to_propagate_statistics_trampoline,
    CopyToPropagateStatistics::propagate_statistics,
    CopyToPropagateStatisticsInfo,
    duckdb_v2_copy_to_propagate_statistics_info_handle
);

info_trampoline!(
    to_file_size_trampoline,
    CopyToFileSize::file_size,
    CopyToFileSizeInfo,
    duckdb_v2_copy_to_file_size_info_handle
);
trampoline!(from_bind_trampoline, CopyFromBind::bind, CopyFromBindInfo, duckdb_v2_copy_from_bind_info_handle);
trampoline!(
    from_init_global_trampoline,
    CopyFromInitGlobal::init_global,
    CopyFromInitGlobalInfo,
    duckdb_v2_copy_from_init_global_info_handle
);
trampoline!(
    from_init_local_trampoline,
    CopyFromInitLocal::init_local,
    CopyFromInitLocalInfo,
    duckdb_v2_copy_from_init_local_info_handle
);
trampoline_cb!(from_exec_trampoline, CopyFromExec::exec, CopyFromExecInfo, duckdb_v2_copy_from_exec_info_handle);
trampoline!(
    from_progress_trampoline,
    CopyFromProgress::progress,
    CopyFromProgressInfo,
    duckdb_v2_copy_from_progress_info_handle
);

/// A copy function under construction (owned until registered).
pub struct CopyFunction {
    raw: duckdb_v2_copy_function_handle,
}

impl CopyFunction {
    pub fn new(factory: Factory) -> Result<CopyFunction> {
        let mut raw = std::ptr::null_mut();
        api_call!(duckdb_v2_copy_function_create(factory.as_raw(), &mut raw));
        Ok(CopyFunction { raw })
    }

    pub fn as_raw(&self) -> duckdb_v2_copy_function_handle {
        self.raw
    }

    /// The FORMAT name.
    pub fn set_name(&self, name: &str) -> Result<&Self> {
        let name = str_arg(name);
        api_call!(duckdb_v2_copy_function_set_name(self.raw, &name));
        Ok(self)
    }

    pub fn set_user_data<T: 'static>(&self, data: T) -> Result<&Self> {
        let mut opaque = Opaque::new(data);
        api_call!(duckdb_v2_copy_function_set_user_data(self.raw, &mut opaque));
        Ok(self)
    }

    /// The file extension (without dot) `COPY ... TO` appends when the target is a directory, e.g. "parquet".
    pub fn set_file_extension(&self, extension: &str) -> Result<&Self> {
        let extension = str_arg(extension);
        api_call!(duckdb_v2_copy_function_set_file_extension(self.raw, &extension));
        Ok(self)
    }

    /// Whether the engine hands the COPY TO bind callback every option of the statement, the ones the engine
    /// itself applies (`use_tmp_file`, `partition_by`, ...) included, so that the function validates them the way
    /// a C++ copy function's plan callback does (`duckdb_v2_copy_to_set_bind_all_options`, a newer engine entry; a
    /// no-op on an engine without it, see [`CopyFunction::bind_all_options_available`]).
    pub fn set_bind_all_options(&self, bind_all: bool) -> Result<&Self> {
        if !CopyFunction::bind_all_options_available() {
            return Ok(self);
        }
        api_call!(duckdb_v2_copy_to_set_bind_all_options(self.raw, bind_all));
        Ok(self)
    }

    /// Whether the loading engine has `duckdb_v2_copy_to_set_bind_all_options`.
    pub fn bind_all_options_available() -> bool {
        crate::api::api().duckdb_v2_copy_to_set_bind_all_options.is_some()
    }

    pub fn set_to_bind<F: CopyToBind>(&self) -> Result<&Self> {
        api_call!(duckdb_v2_copy_to_set_bind_callback(self.raw, Some(to_bind_trampoline::<F>)));
        Ok(self)
    }
    pub fn set_to_batch_size<F: CopyToBatchSize>(&self) -> Result<&Self> {
        api_call!(duckdb_v2_copy_to_set_batch_size_callback(self.raw, Some(to_batch_size_trampoline::<F>)));
        Ok(self)
    }
    pub fn set_to_init<F: CopyToInit>(&self) -> Result<&Self> {
        api_call!(duckdb_v2_copy_to_set_init_callback(self.raw, Some(to_init_trampoline::<F>)));
        Ok(self)
    }
    pub fn set_to_batch<F: CopyToBatch>(&self) -> Result<&Self> {
        api_call!(duckdb_v2_copy_to_set_batch_callback(self.raw, Some(to_batch_trampoline::<F>)));
        Ok(self)
    }
    pub fn set_to_flush<F: CopyToFlush>(&self) -> Result<&Self> {
        api_call!(duckdb_v2_copy_to_set_flush_callback(self.raw, Some(to_flush_trampoline::<F>)));
        Ok(self)
    }
    pub fn set_to_finalize<F: CopyToFinalize>(&self) -> Result<&Self> {
        api_call!(duckdb_v2_copy_to_set_finalize_callback(self.raw, Some(to_finalize_trampoline::<F>)));
        Ok(self)
    }
    pub fn set_to_statistics<F: CopyToStatistics>(&self) -> Result<&Self> {
        api_call!(duckdb_v2_copy_to_set_statistics_callback(self.raw, Some(to_statistics_trampoline::<F>)));
        Ok(self)
    }
    pub fn set_to_file_size<F: CopyToFileSize>(&self) -> Result<&Self> {
        api_call!(duckdb_v2_copy_to_set_file_size_callback(self.raw, Some(to_file_size_trampoline::<F>)));
        Ok(self)
    }
    pub fn set_to_select<F: CopyToSelect>(&self) -> Result<&Self> {
        api_call!(duckdb_v2_copy_to_set_select_callback(self.raw, Some(to_select_trampoline::<F>)));
        Ok(self)
    }
    pub fn set_to_propagate_statistics<F: CopyToPropagateStatistics>(&self) -> Result<&Self> {
        api_call!(duckdb_v2_copy_to_set_propagate_statistics_callback(
            self.raw,
            Some(to_propagate_statistics_trampoline::<F>)
        ));
        Ok(self)
    }
    /// Reads `COPY ... FROM` through the named multi-file table function, with the columns of the target table as
    /// the schema the files must produce, instead of the `set_from_*` callbacks.
    pub fn set_copy_from_function(&self, table_function: &str) -> Result<&Self> {
        let mut name = str_arg(table_function);
        api_call!(duckdb_v2_copy_function_set_copy_from_function(self.raw, &mut name));
        Ok(self)
    }

    /// Declares an option of the copy function: its type and the side (`COPY TO` / `COPY FROM` / both) it belongs
    /// to; `EXPORT DATABASE` then leaves the write-only options out of the statements `IMPORT DATABASE` runs.
    pub fn add_option(&self, name: &str, type_: &LogicalType, mode: DUCKDB_V2_COPY_OPTION_MODE) -> Result<&Self> {
        let mut name = str_arg(name);
        api_call!(duckdb_v2_copy_function_add_option(self.raw, &mut name, type_.as_raw(), mode));
        Ok(self)
    }

    /// Names a write option a `COPY ... FROM` read through the multi-file function ignores (`EXPORT DATABASE`
    /// carries the options of its `COPY ... TO` statements into the statements `IMPORT DATABASE` runs).
    pub fn ignore_copy_from_option(&self, name: &str) -> Result<&Self> {
        let mut name = str_arg(name);
        api_call!(duckdb_v2_copy_function_ignore_copy_from_option(self.raw, &mut name));
        Ok(self)
    }

    pub fn set_from_bind<F: CopyFromBind>(&self) -> Result<&Self> {
        api_call!(duckdb_v2_copy_from_set_bind_callback(self.raw, Some(from_bind_trampoline::<F>)));
        Ok(self)
    }
    pub fn set_from_init_global<F: CopyFromInitGlobal>(&self) -> Result<&Self> {
        api_call!(duckdb_v2_copy_from_set_init_global_callback(self.raw, Some(from_init_global_trampoline::<F>)));
        Ok(self)
    }
    pub fn set_from_init_local<F: CopyFromInitLocal>(&self) -> Result<&Self> {
        api_call!(duckdb_v2_copy_from_set_init_local_callback(self.raw, Some(from_init_local_trampoline::<F>)));
        Ok(self)
    }
    pub fn set_from_exec<F: CopyFromExec>(&self) -> Result<&Self> {
        api_call!(duckdb_v2_copy_from_set_exec_callback(self.raw, Some(from_exec_trampoline::<F>)));
        Ok(self)
    }
    pub fn set_from_progress<F: CopyFromProgress>(&self) -> Result<&Self> {
        api_call!(duckdb_v2_copy_from_set_progress_callback(self.raw, Some(from_progress_trampoline::<F>)));
        Ok(self)
    }
}

impl Drop for CopyFunction {
    fn drop(&mut self) {
        api_destroy!(duckdb_v2_copy_function_destroy(&mut self.raw));
    }
}

macro_rules! data_getter {
    ($(#[$doc:meta])* $name:ident, $api:ident) => {
        $(#[$doc])*
        #[inline]
        pub fn $name<T>(&self) -> Result<Option<&T>> {
            let mut data: *mut c_void = std::ptr::null_mut();
            api_call!($api(self.raw, &mut data));
            // SAFETY(ffi L8.I10): the slot holds what this crate stored through `Opaque` for the `T` the caller names, or is empty,
            // and the engine keeps it alive for the callback (L8.I9); a shared slot (bind data: "stored with the bound
            // call site and retrievable from every later callback"; global state: "every thread reading the file shares
            // it, so the function must synchronize its own access to it") is read through `&T` - the extension's own
            // synchronization is behind that reference, never a `&mut` to the slot.
            Ok(unsafe { Opaque::get::<T>(data) })
        }
    };
}

macro_rules! bind_columns_and_options {
    ($count:ident, $name:ident, $type:ident, $path:ident, $ocount:ident, $oname:ident, $ovalue:ident) => {
        /// The target columns as `(name, type)`.
        pub fn columns(&self) -> Result<Vec<(String, LogicalType)>> {
            let mut count: idx_t = 0;
            api_call!($count(self.raw, &mut count));
            let mut columns = Vec::with_capacity(count as usize);
            for i in 0..count {
                let mut name = duckdb_v2_str::EMPTY;
                api_call!($name(self.raw, i, &mut name));
                let mut type_raw: duckdb_v2_logical_type_handle = std::ptr::null_mut();
                api_call!($type(self.raw, i, &mut type_raw));
                // SAFETY(ffi L8.I1): the entry returns an owned type handle per column.
                columns.push((str_to_string(name), unsafe { LogicalType::from_raw(type_raw) }));
            }
            Ok(columns)
        }

        /// The file path of the COPY.
        pub fn file_path(&self) -> Result<String> {
            let mut path = duckdb_v2_str::EMPTY;
            api_call!($path(self.raw, &mut path));
            Ok(str_to_string(path))
        }

        /// The `(option, value)` pairs of the COPY.
        pub fn options(&self) -> Result<Vec<(String, Value)>> {
            let mut count: idx_t = 0;
            api_call!($ocount(self.raw, &mut count));
            let mut options = Vec::with_capacity(count as usize);
            for i in 0..count {
                let mut name = duckdb_v2_str::EMPTY;
                api_call!($oname(self.raw, i, &mut name));
                let mut value: duckdb_v2_value_handle = std::ptr::null_mut();
                api_call!($ovalue(self.raw, i, &mut value));
                // SAFETY(ffi L8.I1): the entry returns an owned value handle per option.
                options.push((str_to_string(name), unsafe { Value::from_raw(value) }));
            }
            Ok(options)
        }
    };
}

/// The COPY TO bind callback's info.
#[derive(Clone, Copy)]
pub struct CopyToBindInfo {
    raw: duckdb_v2_copy_to_bind_info_handle,
}

impl CopyToBindInfo {
    data_getter!(/// The function's user data.
        user_data, duckdb_v2_copy_to_bind_get_user_data);
    bind_columns_and_options!(
        duckdb_v2_copy_to_bind_get_column_count,
        duckdb_v2_copy_to_bind_get_column_name,
        duckdb_v2_copy_to_bind_get_column_type,
        duckdb_v2_copy_to_bind_get_file_path,
        duckdb_v2_copy_to_bind_get_option_count,
        duckdb_v2_copy_to_bind_get_option_name,
        duckdb_v2_copy_to_bind_get_option_value
    );

    pub fn set_bind_data<T: 'static>(&self, data: T) -> Result<()> {
        let mut opaque = Opaque::new(data);
        api_call!(duckdb_v2_copy_to_bind_set_bind_data(self.raw, &mut opaque));
        Ok(())
    }

    /// Whether the statement is a `COPY ... TO` or one `EXPORT DATABASE` runs (whose files are read back).
    pub fn copy_to_type(&self) -> Result<DUCKDB_V2_COPY_TO_TYPE> {
        let mut copy_to_type = DUCKDB_V2_COPY_TO_TYPE_COPY_TO_FILE;
        api_call!(duckdb_v2_copy_to_bind_get_copy_to_type(self.raw, &mut copy_to_type));
        Ok(copy_to_type)
    }
}

/// The batch-size callback's info.
#[derive(Clone, Copy)]
pub struct CopyToBatchSizeInfo {
    raw: duckdb_v2_copy_to_batch_size_info_handle,
}

impl CopyToBatchSizeInfo {
    data_getter!(/// The function's user data.
        user_data, duckdb_v2_copy_to_batch_size_get_user_data);
    data_getter!(/// The bind data.
        bind_data, duckdb_v2_copy_to_batch_size_get_bind_data);

    pub fn set_target(&self, rows: usize) -> Result<()> {
        api_call!(duckdb_v2_copy_to_batch_size_set_target(self.raw, rows as idx_t));
        Ok(())
    }
}

/// The COPY TO init callback's info.
#[derive(Clone, Copy)]
pub struct CopyToInitInfo {
    raw: duckdb_v2_copy_to_init_info_handle,
}

impl CopyToInitInfo {
    data_getter!(/// The function's user data.
        user_data, duckdb_v2_copy_to_init_get_user_data);
    data_getter!(/// The bind data.
        bind_data, duckdb_v2_copy_to_init_get_bind_data);

    pub fn file_path(&self) -> Result<String> {
        let mut path = duckdb_v2_str::EMPTY;
        api_call!(duckdb_v2_copy_to_init_get_file_path(self.raw, &mut path));
        Ok(str_to_string(path))
    }

    pub fn set_init_data<T: 'static>(&self, data: T) -> Result<()> {
        let mut opaque = Opaque::new(data);
        api_call!(duckdb_v2_copy_to_init_set_init_data(self.raw, &mut opaque));
        Ok(())
    }
}

/// The batch callback's info.
#[derive(Clone, Copy)]
pub struct CopyToBatchInfo {
    raw: duckdb_v2_copy_to_batch_info_handle,
}

impl CopyToBatchInfo {
    data_getter!(/// The function's user data.
        user_data, duckdb_v2_copy_to_batch_get_user_data);
    data_getter!(/// The bind data.
        bind_data, duckdb_v2_copy_to_batch_get_bind_data);
    data_getter!(/// The init data.
        init_data, duckdb_v2_copy_to_batch_get_init_data);

    /// Takes the rows of the batch (a column data collection the callback owns from then on).
    pub fn take_input(&self) -> Result<ColumnDataCollection> {
        let mut raw: duckdb_v2_column_data_collection_handle = std::ptr::null_mut();
        api_call!(duckdb_v2_copy_to_batch_take_input(self.raw, &mut raw));
        // SAFETY(ffi L8.I1): the engine hands over ownership of the collection through the out-parameter.
        Ok(unsafe { ColumnDataCollection::from_raw(raw) })
    }

    pub fn set_batch_data<T: 'static>(&self, data: T) -> Result<()> {
        let mut opaque = Opaque::new(data);
        api_call!(duckdb_v2_copy_to_batch_set_batch_data(self.raw, &mut opaque));
        Ok(())
    }
}

/// The flush callback's info.
pub struct CopyToFlushInfo<'cb> {
    raw: duckdb_v2_copy_to_flush_info_handle,
    /// The exclusive slots of this callback ([`CallbackSlots`]).
    slots: CallbackSlots<'cb>,
}

impl<'cb> CopyToFlushInfo<'cb> {
    data_getter!(/// The function's user data.
        user_data, duckdb_v2_copy_to_flush_get_user_data);
    data_getter!(/// The bind data.
        bind_data, duckdb_v2_copy_to_flush_get_bind_data);
    data_getter!(/// The init data.
        init_data, duckdb_v2_copy_to_flush_get_init_data);
    exclusive_getter!(/// The batch data of the batch to write.
        /// (`duckdb_v2.h`: "flushes of the same file never run concurrently"; the batch is flushed once); handed out once per callback as `&mut` for the callback's duration.
        batch_data, duckdb_v2_copy_to_flush_get_batch_data, BATCH_DATA, "batch_data");
}

/// The finalize callback's info.
#[derive(Clone, Copy)]
pub struct CopyToFinalizeInfo {
    raw: duckdb_v2_copy_to_finalize_info_handle,
}

impl CopyToFinalizeInfo {
    data_getter!(/// The function's user data.
        user_data, duckdb_v2_copy_to_finalize_get_user_data);
    data_getter!(/// The bind data.
        bind_data, duckdb_v2_copy_to_finalize_get_bind_data);
    data_getter!(/// The init data.
        init_data, duckdb_v2_copy_to_finalize_get_init_data);
}

/// The statistics callback's info.
#[derive(Clone, Copy)]
pub struct CopyToStatisticsInfo {
    raw: duckdb_v2_copy_to_statistics_info_handle,
}

impl CopyToStatisticsInfo {
    data_getter!(/// The function's user data.
        user_data, duckdb_v2_copy_to_statistics_get_user_data);
    data_getter!(/// The bind data.
        bind_data, duckdb_v2_copy_to_statistics_get_bind_data);
    data_getter!(/// The init data.
        init_data, duckdb_v2_copy_to_statistics_get_init_data);

    pub fn set_file_size(&self, bytes: usize) -> Result<()> {
        api_call!(duckdb_v2_copy_to_statistics_set_file_size(self.raw, bytes as idx_t));
        Ok(())
    }

    pub fn set_row_count(&self, rows: usize) -> Result<()> {
        api_call!(duckdb_v2_copy_to_statistics_set_row_count(self.raw, rows as idx_t));
        Ok(())
    }

    pub fn set_footer_size(&self, bytes: usize) -> Result<()> {
        api_call!(duckdb_v2_copy_to_statistics_set_footer_size(self.raw, bytes as idx_t));
        Ok(())
    }

    /// A format-specific statistic of the file (`extra_info` of RETURN_STATS).
    pub fn set_extra_info(&self, name: &str, value: &Value) -> Result<()> {
        let name = str_arg(name);
        api_call!(duckdb_v2_copy_to_statistics_set_extra_info(self.raw, &name, value.as_raw()));
        Ok(())
    }

    /// One statistic of one column (`column_statistics` of RETURN_STATS); `column` is the quoted column name or
    /// nested field path, e.g. `"s"."x"`.
    pub fn set_column_statistic(&self, column: &str, name: &str, value: &Value) -> Result<()> {
        let column = str_arg(column);
        let name = str_arg(name);
        api_call!(duckdb_v2_copy_to_statistics_set_column_statistic(self.raw, &column, &name, value.as_raw()));
        Ok(())
    }
}

/// The file size callback's info.
#[derive(Clone, Copy)]
pub struct CopyToFileSizeInfo {
    raw: duckdb_v2_copy_to_file_size_info_handle,
}

impl CopyToFileSizeInfo {
    data_getter!(/// The function's user data.
        user_data, duckdb_v2_copy_to_file_size_get_user_data);
    data_getter!(/// The bind data.
        bind_data, duckdb_v2_copy_to_file_size_get_bind_data);
    data_getter!(/// The init data.
        init_data, duckdb_v2_copy_to_file_size_get_init_data);

    pub fn set_file_size(&self, bytes: usize) -> Result<()> {
        api_call!(duckdb_v2_copy_to_file_size_set_file_size(self.raw, bytes as idx_t));
        Ok(())
    }
}

/// The propagate statistics callback's info.
pub struct CopyToPropagateStatisticsInfo<'cb> {
    raw: duckdb_v2_copy_to_propagate_statistics_info_handle,
    /// The exclusive slots of this callback ([`CallbackSlots`]).
    slots: CallbackSlots<'cb>,
}

impl<'cb> CopyToPropagateStatisticsInfo<'cb> {
    data_getter!(/// The function's user data.
        user_data, duckdb_v2_copy_to_propagate_statistics_get_user_data);
    exclusive_getter!(/// The bind data, to record what the statistics tell in (`duckdb_v2.h`: the callback "can use it to
        /// learn e.g. which columns cannot hold NULL values, and to record that in the bind data"; the optimizer
        /// runs it on the planner thread that owns the call site); handed out once per callback as `&mut`.
        bind_data, duckdb_v2_copy_to_propagate_statistics_get_bind_data, BIND_DATA, "bind_data");

    /// Per written column, whether the statistics allow NULL values (true without statistics).
    pub fn columns_can_have_null(&self) -> Result<Vec<bool>> {
        let mut count: idx_t = 0;
        api_call!(duckdb_v2_copy_to_propagate_statistics_get_column_count(self.raw, &mut count));
        let mut result = Vec::with_capacity(count as usize);
        for i in 0..count {
            let mut can_have_null = true;
            api_call!(duckdb_v2_copy_to_propagate_statistics_column_can_have_null(self.raw, i, &mut can_have_null));
            result.push(can_have_null);
        }
        Ok(result)
    }
}

/// The select callback's info.
#[derive(Clone, Copy)]
pub struct CopyToSelectInfo {
    raw: duckdb_v2_copy_to_select_info_handle,
}

impl CopyToSelectInfo {
    data_getter!(/// The function's user data.
        user_data, duckdb_v2_copy_to_select_get_user_data);

    /// Whether the statement is a regular COPY TO or an EXPORT DATABASE.
    pub fn copy_to_type(&self) -> Result<CopyToType> {
        let mut out = DUCKDB_V2_COPY_TO_TYPE_COPY_TO_FILE;
        api_call!(duckdb_v2_copy_to_select_get_copy_to_type(self.raw, &mut out));
        Ok(if out == DUCKDB_V2_COPY_TO_TYPE_EXPORT_DATABASE {
            CopyToType::ExportDatabase
        } else {
            CopyToType::CopyToFile
        })
    }

    /// The columns being written as `(name, type)`.
    pub fn columns(&self) -> Result<Vec<(String, LogicalType)>> {
        let mut count: idx_t = 0;
        api_call!(duckdb_v2_copy_to_select_get_column_count(self.raw, &mut count));
        let mut columns = Vec::with_capacity(count as usize);
        for i in 0..count {
            let mut name = duckdb_v2_str::EMPTY;
            api_call!(duckdb_v2_copy_to_select_get_column_name(self.raw, i, &mut name));
            let mut type_raw: duckdb_v2_logical_type_handle = std::ptr::null_mut();
            api_call!(duckdb_v2_copy_to_select_get_column_type(self.raw, i, &mut type_raw));
            // SAFETY(ffi L8.I1): the entry returns an owned type handle per column.
            columns.push((str_to_string(name), unsafe { LogicalType::from_raw(type_raw) }));
        }
        Ok(columns)
    }

    /// Requests that column `index` is cast to `type_` before it is written.
    pub fn set_column_type(&self, index: usize, type_: &LogicalType) -> Result<()> {
        api_call!(duckdb_v2_copy_to_select_set_column_type(self.raw, index as idx_t, type_.as_raw()));
        Ok(())
    }

    /// Whether the engine reports the statement's options to the select callback (newer engines only).
    pub fn has_options(&self) -> bool {
        crate::api::api().duckdb_v2_copy_to_select_get_option_count.is_some()
    }

    /// The options of the statement as `(name, value)`, ordered by name - every option, the engine's included; a
    /// bare option reads as `true`, a list as a tuple (an unnamed STRUCT).
    pub fn options(&self) -> Result<Vec<(String, Value)>> {
        let mut count: idx_t = 0;
        api_call!(duckdb_v2_copy_to_select_get_option_count(self.raw, &mut count));
        let mut options = Vec::with_capacity(count as usize);
        for i in 0..count {
            let mut name = duckdb_v2_str::EMPTY;
            api_call!(duckdb_v2_copy_to_select_get_option_name(self.raw, i, &mut name));
            let mut raw: duckdb_v2_value_handle = std::ptr::null_mut();
            api_call!(duckdb_v2_copy_to_select_get_option_value(self.raw, i, &mut raw));
            // SAFETY(ffi L8.I1): the entry returns an owned value handle per option.
            options.push((str_to_string(name), unsafe { Value::from_raw(raw) }));
        }
        Ok(options)
    }

    /// Requests that column `index` is written as `function_name(column, arguments...)`: a call of the built-in
    /// scalar function with the column as its first argument and the values as constant arguments after it, bound
    /// like a call written in the query; the column keeps its name (newer engines only).
    pub fn set_column_function(&self, index: usize, function_name: &str, arguments: &[&Value]) -> Result<()> {
        let name = str_arg(function_name);
        let mut raw: Vec<duckdb_v2_value_handle> = arguments.iter().map(|value| value.as_raw()).collect();
        api_call!(duckdb_v2_copy_to_select_set_column_function(
            self.raw,
            index as idx_t,
            &name,
            raw.as_mut_ptr(),
            raw.len() as idx_t
        ));
        Ok(())
    }
}

/// The COPY FROM bind callback's info.
#[derive(Clone, Copy)]
pub struct CopyFromBindInfo {
    raw: duckdb_v2_copy_from_bind_info_handle,
}

impl CopyFromBindInfo {
    data_getter!(/// The function's user data.
        user_data, duckdb_v2_copy_from_bind_get_user_data);
    bind_columns_and_options!(
        duckdb_v2_copy_from_bind_get_column_count,
        duckdb_v2_copy_from_bind_get_column_name,
        duckdb_v2_copy_from_bind_get_column_type,
        duckdb_v2_copy_from_bind_get_file_path,
        duckdb_v2_copy_from_bind_get_option_count,
        duckdb_v2_copy_from_bind_get_option_name,
        duckdb_v2_copy_from_bind_get_option_value
    );

    pub fn set_bind_data<T: 'static>(&self, data: T) -> Result<()> {
        let mut opaque = Opaque::new(data);
        api_call!(duckdb_v2_copy_from_bind_set_bind_data(self.raw, &mut opaque));
        Ok(())
    }

    pub fn set_cardinality(&self, cardinality: usize, is_exact: bool) -> Result<()> {
        api_call!(duckdb_v2_copy_from_bind_set_cardinality(self.raw, cardinality as idx_t, is_exact));
        Ok(())
    }
}

/// The COPY FROM global init callback's info.
#[derive(Clone, Copy)]
pub struct CopyFromInitGlobalInfo {
    raw: duckdb_v2_copy_from_init_global_info_handle,
}

impl CopyFromInitGlobalInfo {
    data_getter!(/// The function's user data.
        user_data, duckdb_v2_copy_from_init_global_get_user_data);
    data_getter!(/// The bind data.
        bind_data, duckdb_v2_copy_from_init_global_get_bind_data);

    pub fn set_global_state<T: 'static>(&self, state: T) -> Result<()> {
        let mut opaque = Opaque::new(state);
        api_call!(duckdb_v2_copy_from_init_global_set_global_state(self.raw, &mut opaque));
        Ok(())
    }

    pub fn set_max_threads(&self, max_threads: usize) -> Result<()> {
        api_call!(duckdb_v2_copy_from_init_global_set_max_threads(self.raw, max_threads as idx_t));
        Ok(())
    }
}

/// The COPY FROM local init callback's info.
#[derive(Clone, Copy)]
pub struct CopyFromInitLocalInfo {
    raw: duckdb_v2_copy_from_init_local_info_handle,
}

impl CopyFromInitLocalInfo {
    data_getter!(/// The function's user data.
        user_data, duckdb_v2_copy_from_init_local_get_user_data);
    data_getter!(/// The bind data.
        bind_data, duckdb_v2_copy_from_init_local_get_bind_data);
    data_getter!(/// The global state.
        global_state, duckdb_v2_copy_from_init_local_get_global_state);

    pub fn set_local_state<T: 'static>(&self, state: T) -> Result<()> {
        let mut opaque = Opaque::new(state);
        api_call!(duckdb_v2_copy_from_init_local_set_local_state(self.raw, &mut opaque));
        Ok(())
    }
}

/// The COPY FROM exec callback's info.
pub struct CopyFromExecInfo<'cb> {
    raw: duckdb_v2_copy_from_exec_info_handle,
    /// The exclusive slots of this callback ([`CallbackSlots`]).
    slots: CallbackSlots<'cb>,
}

impl<'cb> CopyFromExecInfo<'cb> {
    data_getter!(/// The function's user data.
        user_data, duckdb_v2_copy_from_exec_get_user_data);
    data_getter!(/// The bind data.
        bind_data, duckdb_v2_copy_from_exec_get_bind_data);
    data_getter!(/// The global state.
        global_state, duckdb_v2_copy_from_exec_get_global_state);
    exclusive_getter!(/// This thread's local state.
        /// (`duckdb_v2.h`: "worker-local state"); handed out once per callback as `&mut` for the callback's duration.
        local_state, duckdb_v2_copy_from_exec_get_local_state, LOCAL_STATE, "local_state");

    /// The chunk to fill (borrowed); size 0 ends the read.
    pub fn output(&self) -> Result<DataChunk> {
        Ok(api_call_out!(duckdb_v2_copy_from_exec_get_output_chunk(self.raw) -> DataChunk::borrowed))
    }
}

/// The COPY FROM progress callback's info.
#[derive(Clone, Copy)]
pub struct CopyFromProgressInfo {
    raw: duckdb_v2_copy_from_progress_info_handle,
}

impl CopyFromProgressInfo {
    data_getter!(/// The function's user data.
        user_data, duckdb_v2_copy_from_progress_get_user_data);
    data_getter!(/// The bind data.
        bind_data, duckdb_v2_copy_from_progress_get_bind_data);
    data_getter!(/// The global state.
        global_state, duckdb_v2_copy_from_progress_get_global_state);

    pub fn set_progress(&self, progress: f64) -> Result<()> {
        api_call!(duckdb_v2_copy_from_progress_set_progress(self.raw, progress));
        Ok(())
    }
}
