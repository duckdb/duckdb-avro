//! Table functions and the multi-file wrapper around a single-file table function.

use std::ffi::c_void;

use crate::Result;
use crate::api;
use crate::chunk::DataChunk;
use crate::context::{Context, Factory};
use crate::error::guard;
use crate::expression::Expression;
use crate::function::{BindInfo, FunctionDocs, FunctionSignature};
use crate::opaque::{CallbackSlots, Opaque};
use crate::query::{ResultType, Statement};
use crate::statistics::{Statistics, Stats};
use crate::sys::*;
use crate::text::{str_arg, str_to_string};
use crate::types::LogicalType;
use crate::value::Value;
use crate::vector::Vector;

/// `DUCKDB_V2_ORDER_PRESERVATION` (`NO_ORDER` / `INSERTION_ORDER` / `FIXED_ORDER`).
pub type OrderPreservation = DUCKDB_V2_ORDER_PRESERVATION;
/// `DUCKDB_V2_TABLE_PARTITION_INFO`.
pub type TablePartitionInfo = DUCKDB_V2_TABLE_PARTITION_INFO;
/// `DUCKDB_V2_OPERATOR_RESULT`: what an in-out callback reports about its output.
pub type OperatorResult = DUCKDB_V2_OPERATOR_RESULT;
/// `DUCKDB_V2_TABLE_FUNCTION_RETURN_TYPE`: a table function or a set-returning function.
pub type TableFunctionReturnType = DUCKDB_V2_TABLE_FUNCTION_RETURN_TYPE;

/// Declares the result columns of a call and stores bind data.
pub trait TableBind {
    fn bind(bind: &BindInfo, info: &TableBindInfo, context: &Context) -> Result<()>;
}
/// Creates the state shared by every thread of a scan.
pub trait TableInitGlobal {
    fn init_global(info: &TableInitGlobalInfo, context: &Context) -> Result<()>;
}
/// Creates the per-thread state of a scan.
pub trait TableInitLocal {
    fn init_local(info: &TableInitLocalInfo, context: &Context) -> Result<()>;
}
/// Produces the next output chunk; an empty chunk ends the scan of this thread.
pub trait TableExec {
    fn exec(info: &TableExecInfo<'_>, context: &Context) -> Result<()>;
}
/// Reports the scan progress (0.0 to 1.0).
/// A table in-out function (`table_in_out_function_t`): consumes the input chunk's rows and produces rows, called
/// again with the same input while it reports `HAVE_MORE_OUTPUT`.
pub trait TableInOut {
    fn in_out(info: &TableInOutInfo<'_>, context: &Context) -> Result<OperatorResult>;
}

pub trait TableProgress {
    fn progress(info: &TableProgressInfo, context: &Context) -> Result<()>;
}
/// Claims the next batch for a local state, so a multi-file scan can order the batches.
pub trait TableClaimBatch {
    fn claim_batch(info: &TableClaimBatchInfo<'_>, context: &Context) -> Result<()>;
}
/// Schedules the I/O of the batch a local state claimed: tasks the engine runs before the batch is scanned.
pub trait TableScheduleIo {
    fn schedule_io(info: &TableScheduleIoInfo<'_>, context: &Context) -> Result<()>;
}
/// Prepares a file a read-ahead scan opened for its batches: pre-opens what claiming them needs, on the pool thread
/// that opened the file (`table_function_prepare_read_ahead_t`).
pub trait TablePrepareReadAhead {
    fn prepare_read_ahead(info: &TablePrepareReadAheadInfo, context: &Context) -> Result<()>;
}
/// Answers the optimizer whether the scan extracts nested fields of a declared STRUCT / VARIANT column itself
/// (`table_function_supports_pushdown_extract_t`); asked while the plan is optimized, without a client context.
pub trait TableSupportsPushdownExtract {
    fn supports_pushdown_extract(info: &TablePushdownExtractInfo) -> Result<()>;
}
/// Reports the batch index (and partition values) of the chunk a local state just produced.
pub trait TablePartitionData {
    fn partition_data(info: &TablePartitionDataInfo<'_>, context: &Context) -> Result<()>;
}
/// Tells the planner how the function's output is partitioned over the given columns.
pub trait TablePartitioning {
    fn partitioning(info: &TablePartitioningInfo, context: &Context) -> Result<()>;
}
/// Accepts filters the function can evaluate itself.
pub trait TableFilterPushdown {
    fn filter_pushdown(info: &TableFilterPushdownInfo, context: &Context) -> Result<()>;
}
/// Describes a bound call (column identifiers, options) for callers such as the multi-file scan.
pub trait TableGetBindInfo {
    fn get_bind_info(info: &TableGetBindInfoInfo, context: &Context) -> Result<()>;
}
/// Reports the profiler metrics of a scan (row groups scanned / to scan).
pub trait TableGetMetrics {
    fn get_metrics(info: &TableGetMetricsInfo, context: &Context) -> Result<()>;
}
/// Reports the statistics of a column of a bound call (see [`Statistics`]); reporting nothing leaves them unknown.
pub trait TableStatistics {
    fn statistics(info: &TableStatisticsInfo, context: &Context) -> Result<()>;
}
/// Called while the query is optimized, once per declared column the query reads, to narrow that column's
/// statistics ([`Stats`]). Optional; without it the engine knows nothing about the columns. Unstable API.
pub trait TableStats {
    fn stats(info: &TableStatsInfo, context: &Context) -> Result<()>;
}
/// Reports the partitions (row groups) of a bound call, in scan order; reporting none leaves them unknown.
pub trait TablePartitionStats {
    fn partition_stats(info: &TablePartitionStatsInfo, context: &Context) -> Result<()>;
}
/// Reports the statistics of one column within one partition; reporting nothing leaves them unknown.
pub trait TablePartitionColumnStatistics {
    fn partition_column_statistics(info: &TablePartitionColumnStatisticsInfo, context: &Context) -> Result<()>;
}
/// Decides whether the scan computes an expression over one of its columns itself.
pub trait TableProjectionExpressionPushdown {
    fn projection_expression_pushdown(info: &TableProjectionExpressionPushdownInfo, context: &Context) -> Result<()>;
}
/// Binds a call as a query in its place (`TableFunction::bind_replace`): the SQL or statement the call becomes.
pub trait TableBindReplace {
    fn bind_replace(bind: &BindInfo, info: &TableBindReplaceInfo, context: &Context) -> Result<()>;
}

info_trampoline!(
    supports_pushdown_extract_trampoline,
    TableSupportsPushdownExtract::supports_pushdown_extract,
    TablePushdownExtractInfo,
    duckdb_v2_table_function_pushdown_extract_info_handle
);
bind_trampoline!(bind_trampoline, TableBind::bind, TableBindInfo, duckdb_v2_table_function_bind_info_handle);
bind_trampoline!(
    bind_replace_trampoline,
    TableBindReplace::bind_replace,
    TableBindReplaceInfo,
    duckdb_v2_table_function_bind_replace_info_handle
);
trampoline!(
    init_global_trampoline,
    TableInitGlobal::init_global,
    TableInitGlobalInfo,
    duckdb_v2_table_function_init_global_info_handle
);
trampoline!(
    init_local_trampoline,
    TableInitLocal::init_local,
    TableInitLocalInfo,
    duckdb_v2_table_function_init_local_info_handle
);
trampoline_cb!(exec_trampoline, TableExec::exec, TableExecInfo, duckdb_v2_table_function_exec_info_handle);
unsafe extern "C" fn in_out_trampoline<F: TableInOut>(
    info: duckdb_v2_table_function_in_out_info_handle,
    context: duckdb_v2_context_handle,
    err: *mut duckdb_v2_error_info_handle,
) {
    // the exclusive slots are handed out for this frame only
    let scope = ();
    guard(err, || {
        let info = TableInOutInfo { raw: info, slots: CallbackSlots::scoped(&scope) };
        // SAFETY(ffi L8.I1): the engine lends a live context for the duration of the callback.
        let context = unsafe { Context::from_raw(context) };
        let result = F::in_out(&info, &context)?;
        info.set_result(result)
    });
}
trampoline!(
    progress_trampoline,
    TableProgress::progress,
    TableProgressInfo,
    duckdb_v2_table_function_progress_info_handle
);
trampoline_cb!(
    claim_batch_trampoline,
    TableClaimBatch::claim_batch,
    TableClaimBatchInfo,
    duckdb_v2_table_function_claim_batch_info_handle
);
trampoline_cb!(
    schedule_io_trampoline,
    TableScheduleIo::schedule_io,
    TableScheduleIoInfo,
    duckdb_v2_table_function_schedule_io_info_handle
);
trampoline!(
    prepare_read_ahead_trampoline,
    TablePrepareReadAhead::prepare_read_ahead,
    TablePrepareReadAheadInfo,
    duckdb_v2_table_function_prepare_read_ahead_info_handle
);
trampoline_cb!(
    partition_data_trampoline,
    TablePartitionData::partition_data,
    TablePartitionDataInfo,
    duckdb_v2_table_function_partition_data_info_handle
);
trampoline!(
    partitioning_trampoline,
    TablePartitioning::partitioning,
    TablePartitioningInfo,
    duckdb_v2_table_function_partitioning_info_handle
);
trampoline!(
    filter_pushdown_trampoline,
    TableFilterPushdown::filter_pushdown,
    TableFilterPushdownInfo,
    duckdb_v2_table_function_filter_pushdown_info_handle
);
trampoline!(
    get_bind_info_trampoline,
    TableGetBindInfo::get_bind_info,
    TableGetBindInfoInfo,
    duckdb_v2_table_function_get_bind_info_info_handle
);
trampoline!(
    get_metrics_trampoline,
    TableGetMetrics::get_metrics,
    TableGetMetricsInfo,
    duckdb_v2_table_function_get_metrics_info_handle
);
trampoline!(
    statistics_trampoline,
    TableStatistics::statistics,
    TableStatisticsInfo,
    duckdb_v2_table_function_statistics_info_handle
);
trampoline!(stats_trampoline, TableStats::stats, TableStatsInfo, duckdb_v2_table_function_stats_info_handle);
trampoline!(
    partition_stats_trampoline,
    TablePartitionStats::partition_stats,
    TablePartitionStatsInfo,
    duckdb_v2_table_function_partition_stats_info_handle
);
trampoline!(
    partition_column_statistics_trampoline,
    TablePartitionColumnStatistics::partition_column_statistics,
    TablePartitionColumnStatisticsInfo,
    duckdb_v2_table_function_partition_column_statistics_info_handle
);
trampoline!(
    projection_expression_pushdown_trampoline,
    TableProjectionExpressionPushdown::projection_expression_pushdown,
    TableProjectionExpressionPushdownInfo,
    duckdb_v2_table_function_projection_expression_pushdown_info_handle
);

/// A table function under construction (owned until registered).
pub struct TableFunction {
    raw: duckdb_v2_table_function_handle,
}

impl TableFunction {
    pub fn new(factory: Factory) -> Result<TableFunction> {
        let mut raw = std::ptr::null_mut();
        api_call!(duckdb_v2_table_function_create(factory.as_raw(), &mut raw));
        Ok(TableFunction { raw })
    }

    pub fn as_raw(&self) -> duckdb_v2_table_function_handle {
        self.raw
    }

    pub fn set_name(&self, name: &str) -> Result<&Self> {
        let name = str_arg(name);
        api_call!(duckdb_v2_table_function_set_name(self.raw, &name));
        Ok(self)
    }

    pub fn signature(&self) -> Result<FunctionSignature> {
        Ok(api_call_out!(duckdb_v2_table_function_get_signature(self.raw) -> FunctionSignature::from_raw))
    }

    pub fn docs(&self) -> Result<FunctionDocs> {
        Ok(api_call_out!(duckdb_v2_table_function_get_docs(self.raw) -> FunctionDocs::from_raw))
    }

    pub fn set_user_data<T: 'static>(&self, data: T) -> Result<&Self> {
        let mut opaque = Opaque::new(data);
        api_call!(duckdb_v2_table_function_set_user_data(self.raw, &mut opaque));
        Ok(self)
    }

    /// Declares a virtual column the function can produce next to its bound columns (e.g. the row number
    /// within a file); returns the column id the scan asks for it under (`init_global_get_column_index`).
    pub fn add_virtual_column(&self, name: &str, type_: &LogicalType) -> Result<idx_t> {
        let mut name = str_arg(name);
        let mut column_id: idx_t = 0;
        api_call!(duckdb_v2_table_function_add_virtual_column(self.raw, &mut name, type_.as_raw(), &mut column_id));
        Ok(column_id)
    }

    /// Whether the function only produces the columns the query reads (it then consults the projected
    /// column list of its global init info).
    pub fn set_projection_pushdown(&self, enabled: bool) -> Result<&Self> {
        api_call!(duckdb_v2_table_function_set_projection_pushdown(self.raw, enabled));
        Ok(self)
    }

    pub fn set_bind<F: TableBind>(&self) -> Result<&Self> {
        api_call!(duckdb_v2_table_function_set_bind_callback(self.raw, Some(bind_trampoline::<F>)));
        Ok(self)
    }
    pub fn set_init_global<F: TableInitGlobal>(&self) -> Result<&Self> {
        api_call!(duckdb_v2_table_function_set_init_global_callback(self.raw, Some(init_global_trampoline::<F>)));
        Ok(self)
    }
    pub fn set_init_local<F: TableInitLocal>(&self) -> Result<&Self> {
        api_call!(duckdb_v2_table_function_set_init_local_callback(self.raw, Some(init_local_trampoline::<F>)));
        Ok(self)
    }
    pub fn set_exec<F: TableExec>(&self) -> Result<&Self> {
        api_call!(duckdb_v2_table_function_set_exec_callback(self.raw, Some(exec_trampoline::<F>)));
        Ok(self)
    }
    /// `TableFunction::in_out_function`: the function consumes rows and produces rows (no exec callback needed).
    pub fn set_in_out<F: TableInOut>(&self) -> Result<&Self> {
        api_call!(duckdb_v2_table_function_set_in_out_callback(self.raw, Some(in_out_trampoline::<F>)));
        Ok(self)
    }
    /// `TableFunction::return_type`: a set-returning function (`range` / `generate_series`) is unnested when it is
    /// referenced like a column. newer engines only: a no-op for a library without the entry.
    pub fn set_return_type(&self, return_type: TableFunctionReturnType) -> Result<&Self> {
        if api::api().duckdb_v2_table_function_set_return_type.is_none() {
            return Ok(self);
        }
        api_call!(duckdb_v2_table_function_set_return_type(self.raw, return_type));
        Ok(self)
    }
    /// `TableFunction::call_return_type`: what a bare call of the function (`CALL f()`, `SELECT * FROM f()`) returns;
    /// `dbgen` / `dsdgen` return nothing, so a shell renders no result for them. newer engines only: a no-op for a
    /// library without the entry.
    pub fn set_call_return_type(&self, return_type: ResultType) -> Result<&Self> {
        if api::api().duckdb_v2_table_function_set_call_return_type.is_none() {
            return Ok(self);
        }
        api_call!(duckdb_v2_table_function_set_call_return_type(self.raw, return_type));
        Ok(self)
    }
    pub fn set_progress<F: TableProgress>(&self) -> Result<&Self> {
        api_call!(duckdb_v2_table_function_set_progress_callback(self.raw, Some(progress_trampoline::<F>)));
        Ok(self)
    }
    pub fn set_claim_batch<F: TableClaimBatch>(&self) -> Result<&Self> {
        api_call!(duckdb_v2_table_function_set_claim_batch_callback(self.raw, Some(claim_batch_trampoline::<F>)));
        Ok(self)
    }
    /// Sets the "schedule io" callback: the I/O of a claimed batch, as tasks the engine runs before scanning it.
    pub fn set_schedule_io<F: TableScheduleIo>(&self) -> Result<&Self> {
        api_call!(duckdb_v2_table_function_set_schedule_io_callback(self.raw, Some(schedule_io_trampoline::<F>)));
        Ok(self)
    }
    /// Sets the "prepare read ahead" callback: called on the engine's I/O pool once per file a read-ahead scan
    /// opened, before its batches are claimed. newer engines only: an error for a library without the entry (check
    /// `api().duckdb_v2_table_function_set_prepare_read_ahead_callback` first).
    pub fn set_prepare_read_ahead<F: TablePrepareReadAhead>(&self) -> Result<&Self> {
        api_call!(duckdb_v2_table_function_set_prepare_read_ahead_callback(
            self.raw,
            Some(prepare_read_ahead_trampoline::<F>)
        ));
        Ok(self)
    }
    /// Sets the "supports pushdown extract" callback: nested fields of STRUCT / VARIANT columns the callback approves
    /// become scan columns of their own (described by the init infos' `column_extract_*` getters). Requires
    /// projection pushdown.
    pub fn set_supports_pushdown_extract<F: TableSupportsPushdownExtract>(&self) -> Result<&Self> {
        api_call!(duckdb_v2_table_function_set_supports_pushdown_extract_callback(
            self.raw,
            Some(supports_pushdown_extract_trampoline::<F>)
        ));
        Ok(self)
    }
    /// Sets the bind-replace callback: every call of the function is bound as the query the callback returns
    /// (`TableFunction::bind_replace`). newer engines only: an error for a library without the entry (see
    /// [`TableFunction::bind_replace_available`]).
    pub fn set_bind_replace<F: TableBindReplace>(&self) -> Result<&Self> {
        api_call!(duckdb_v2_table_function_set_bind_replace_callback(self.raw, Some(bind_replace_trampoline::<F>)));
        Ok(self)
    }
    /// Whether the loading engine binds table functions through a replacement query
    /// (`duckdb_v2_table_function_set_bind_replace_callback` + `bind_replace_set_query`).
    pub fn bind_replace_available() -> bool {
        let api = api::api();
        api.duckdb_v2_table_function_set_bind_replace_callback.is_some()
            && api.duckdb_v2_table_function_bind_replace_set_query.is_some()
    }
    pub fn set_partition_data<F: TablePartitionData>(&self) -> Result<&Self> {
        api_call!(duckdb_v2_table_function_set_partition_data_callback(self.raw, Some(partition_data_trampoline::<F>)));
        Ok(self)
    }
    pub fn set_partitioning<F: TablePartitioning>(&self) -> Result<&Self> {
        api_call!(duckdb_v2_table_function_set_partitioning_callback(self.raw, Some(partitioning_trampoline::<F>)));
        Ok(self)
    }
    pub fn set_filter_pushdown<F: TableFilterPushdown>(&self) -> Result<&Self> {
        api_call!(duckdb_v2_table_function_set_filter_pushdown_callback(
            self.raw,
            Some(filter_pushdown_trampoline::<F>)
        ));
        Ok(self)
    }
    pub fn set_get_bind_info<F: TableGetBindInfo>(&self) -> Result<&Self> {
        api_call!(duckdb_v2_table_function_set_get_bind_info_callback(self.raw, Some(get_bind_info_trampoline::<F>)));
        Ok(self)
    }
    pub fn set_get_metrics<F: TableGetMetrics>(&self) -> Result<&Self> {
        api_call!(duckdb_v2_table_function_set_get_metrics_callback(self.raw, Some(get_metrics_trampoline::<F>)));
        Ok(self)
    }
    pub fn set_statistics<F: TableStatistics>(&self) -> Result<&Self> {
        api_call!(duckdb_v2_table_function_set_statistics_callback(self.raw, Some(statistics_trampoline::<F>)));
        Ok(self)
    }
    /// Lets the function narrow the statistics of the columns it produces: see [`TableStats`]. Unstable API.
    pub fn set_stats<F: TableStats>(&self) -> Result<&Self> {
        api_call!(duckdb_v2_table_function_set_stats_callback(self.raw, Some(stats_trampoline::<F>)));
        Ok(self)
    }
    pub fn set_partition_stats<F: TablePartitionStats>(&self) -> Result<&Self> {
        api_call!(duckdb_v2_table_function_set_partition_stats_callback(
            self.raw,
            Some(partition_stats_trampoline::<F>)
        ));
        Ok(self)
    }
    pub fn set_partition_column_statistics<F: TablePartitionColumnStatistics>(&self) -> Result<&Self> {
        api_call!(duckdb_v2_table_function_set_partition_column_statistics_callback(
            self.raw,
            Some(partition_column_statistics_trampoline::<F>)
        ));
        Ok(self)
    }
    pub fn set_projection_expression_pushdown<F: TableProjectionExpressionPushdown>(&self) -> Result<&Self> {
        api_call!(duckdb_v2_table_function_set_projection_expression_pushdown_callback(
            self.raw,
            Some(projection_expression_pushdown_trampoline::<F>)
        ));
        Ok(self)
    }
    /// Whether columns only a pushed-down filter refers to are left out of the scan (`filter_prune`).
    pub fn set_filter_prune(&self, enabled: bool) -> Result<&Self> {
        api_call!(duckdb_v2_table_function_set_filter_prune(self.raw, enabled));
        Ok(self)
    }
    /// Whether the scan can be split into a row-id scan and a re-fetch by row id; requires row id columns.
    pub fn set_late_materialization(&self, enabled: bool) -> Result<&Self> {
        api_call!(duckdb_v2_table_function_set_late_materialization(self.raw, enabled));
        Ok(self)
    }
    /// Whether arbitrary single-column predicates are pushed into the scan as table filters.
    pub fn set_expression_filter_pushdown(&self, enabled: bool) -> Result<&Self> {
        api_call!(duckdb_v2_table_function_set_expression_filter_pushdown(self.raw, enabled));
        Ok(self)
    }
    /// Declares a virtual column (see [`TableFunction::add_virtual_column`]) as a row id column.
    pub fn add_row_id_column(&self, column_id: idx_t) -> Result<&Self> {
        api_call!(duckdb_v2_table_function_add_row_id_column(self.raw, column_id));
        Ok(self)
    }
}

impl Drop for TableFunction {
    fn drop(&mut self) {
        api_destroy!(duckdb_v2_table_function_destroy(&mut self.raw));
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

/// The columns a scan reads, in output order, from a `(count, index)` getter pair of an info.
macro_rules! column_indexes_getter {
    ($count_api:ident, $index_api:ident) => {
        /// The columns the scan reads (indexes into the declared result columns, or the ids of virtual columns), in
        /// output order.
        pub fn column_indexes(&self) -> Result<Vec<usize>> {
            let mut count: idx_t = 0;
            api_call!($count_api(self.raw, &mut count));
            let mut columns = Vec::with_capacity(count as usize);
            for i in 0..count {
                let mut column: idx_t = 0;
                api_call!($index_api(self.raw, i, &mut column));
                columns.push(column as usize);
            }
            Ok(columns)
        }
    };
}

/// One step of the field path a scan column extracts from its declared column: a STRUCT field by position, or a
/// field of a VARIANT value by key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExtractField {
    Index(usize),
    Name(String),
}

/// The getters describing the field path a scan column extracts (`init_global` / `init_local` infos).
macro_rules! extract_getters {
    ($depth_api:ident, $field_api:ident, $type_api:ident) => {
        /// How many nested fields the scan column at `index` extracts from its declared column: `0` when the
        /// column is read whole.
        pub fn column_extract_depth(&self, index: usize) -> Result<usize> {
            let mut depth: idx_t = 0;
            api_call!($depth_api(self.raw, index as idx_t, &mut depth));
            Ok(depth as usize)
        }

        /// The component at `level` (`0` being a field of the declared column itself) of the path the scan column
        /// at `index` extracts.
        pub fn column_extract_field(&self, index: usize, level: usize) -> Result<ExtractField> {
            let mut by_name = false;
            let mut field_index: idx_t = 0;
            let mut field_name = duckdb_v2_str::EMPTY;
            api_call!($field_api(
                self.raw,
                index as idx_t,
                level as idx_t,
                &mut by_name,
                &mut field_index,
                &mut field_name
            ));
            Ok(if by_name {
                ExtractField::Name(str_to_string(field_name))
            } else {
                ExtractField::Index(field_index as usize)
            })
        }

        /// The type the scan produces the extracted field of the scan column at `index` as (the field's type, or
        /// the target of a cast pushed into the scan).
        pub fn column_extract_type(&self, index: usize) -> Result<LogicalType> {
            let mut borrowed: duckdb_v2_logical_type_handle = std::ptr::null_mut();
            api_call!($type_api(self.raw, index as idx_t, &mut borrowed));
            // the handle is lent for the callback: keep a copy of our own
            Ok(api_call_out!(duckdb_v2_logical_type_copy(borrowed) -> LogicalType::from_raw))
        }
    };
}

/// The bind callback's info: declares the result columns.
#[derive(Clone, Copy)]
pub struct TableBindInfo {
    raw: duckdb_v2_table_function_bind_info_handle,
}

impl TableBindInfo {
    pub fn add_result_column(&self, name: &str, type_: &LogicalType) -> Result<&Self> {
        let name = str_arg(name);
        api_call!(duckdb_v2_table_function_bind_add_result_column(self.raw, &name, type_.as_raw()));
        Ok(self)
    }

    /// The (estimated or exact) row count, for the optimizer.
    pub fn set_cardinality(&self, cardinality: usize, is_exact: bool) -> Result<&Self> {
        api_call!(duckdb_v2_table_function_bind_set_cardinality(self.raw, cardinality as idx_t, is_exact));
        Ok(self)
    }

    pub fn set_order_preservation(&self, order: OrderPreservation) -> Result<&Self> {
        api_call!(duckdb_v2_table_function_bind_set_order_preservation(self.raw, order));
        Ok(self)
    }

    /// `TableFunctionFileBindInput::expected_names/types`: the schema this bind is expected to produce, when the
    /// multi-file scan already determined it (`None` without one; always `None` on a library without the entries).
    pub fn expected_columns(&self) -> Result<Option<Vec<(String, LogicalType)>>> {
        if api::api().duckdb_v2_table_function_bind_get_expected_column_count.is_none() {
            return Ok(None);
        }
        let mut count: idx_t = 0;
        api_call!(duckdb_v2_table_function_bind_get_expected_column_count(self.raw, &mut count));
        if count == 0 {
            return Ok(None);
        }
        let mut columns = Vec::with_capacity(count as usize);
        for i in 0..count {
            let mut name = duckdb_v2_str::EMPTY;
            api_call!(duckdb_v2_table_function_bind_get_expected_column_name(self.raw, i, &mut name));
            let mut raw = std::ptr::null_mut();
            api_call!(duckdb_v2_table_function_bind_get_expected_column_type(self.raw, i, &mut raw));
            // SAFETY(ffi L8.I1): the entry returns an owned type handle.
            columns.push((str_to_string(name), unsafe { LogicalType::from_raw(raw) }));
        }
        Ok(Some(columns))
    }

    /// `TableFunctionFileBindInput::expected_bind_data`: the bind data (of this same function, as `T`) that
    /// determined the expected schema, if any.
    pub fn expected_bind_data<T>(&self) -> Result<Option<&T>> {
        if api::api().duckdb_v2_table_function_bind_get_expected_bind_data.is_none() {
            return Ok(None);
        }
        let mut data: *mut c_void = std::ptr::null_mut();
        api_call!(duckdb_v2_table_function_bind_get_expected_bind_data(self.raw, &mut data));
        // SAFETY(ffi L8.I1): the slot holds what a bind of this function stored, which the caller names as `T`.
        Ok(unsafe { Opaque::get::<T>(data) })
    }
}

/// The bind-replace callback's info: the query (or statement) the call is bound as.
#[derive(Clone, Copy)]
pub struct TableBindReplaceInfo {
    raw: duckdb_v2_table_function_bind_replace_info_handle,
}

impl TableBindReplaceInfo {
    /// The SQL the call is bound as (a subquery).
    pub fn set_query(&self, query: &str) -> Result<()> {
        let query = str_arg(query);
        api_call!(duckdb_v2_table_function_bind_replace_set_query(self.raw, &query));
        Ok(())
    }

    /// The statement itself (its expression locations and bound types stay what they were). An error for a library
    /// without the entry (see [`TableBindReplaceInfo::set_statement_available`]).
    pub fn set_statement(&self, statement: &Statement) -> Result<()> {
        api_call!(duckdb_v2_table_function_bind_replace_set_statement(self.raw, statement.as_raw()));
        Ok(())
    }

    pub fn set_statement_available() -> bool {
        api::api().duckdb_v2_table_function_bind_replace_set_statement.is_some()
    }
}

/// The global init callback's info.
#[derive(Clone, Copy)]
pub struct TableInitGlobalInfo {
    raw: duckdb_v2_table_function_init_global_info_handle,
}

impl TableInitGlobalInfo {
    data_getter!(/// The function's user data.
        user_data, duckdb_v2_table_function_init_global_get_user_data);
    data_getter!(/// The bind data of the call.
        bind_data, duckdb_v2_table_function_init_global_get_bind_data);
    column_indexes_getter!(
        duckdb_v2_table_function_init_global_get_column_count,
        duckdb_v2_table_function_init_global_get_column_index
    );

    /// Stores the state every thread of the scan shares (synchronize access yourself).
    pub fn set_global_state<T: 'static>(&self, state: T) -> Result<()> {
        let mut opaque = Opaque::new(state);
        api_call!(duckdb_v2_table_function_init_global_set_global_state(self.raw, &mut opaque));
        Ok(())
    }

    /// The maximum number of threads that may scan concurrently.
    pub fn set_max_threads(&self, max_threads: usize) -> Result<()> {
        api_call!(duckdb_v2_table_function_init_global_set_max_threads(self.raw, max_threads as idx_t));
        Ok(())
    }

    /// The number of table filters pushed into the scan (one per filtered scan column).
    pub fn filter_count(&self) -> Result<usize> {
        let mut count: idx_t = 0;
        api_call!(duckdb_v2_table_function_init_global_get_filter_count(self.raw, &mut count));
        Ok(count as usize)
    }

    /// A pushed table filter as a predicate over the scan's columns (column references index the
    /// scan's output positions). Borrowed for the callback.
    pub fn filter(&self, index: usize) -> Result<Expression> {
        Ok(
            api_call_out!(duckdb_v2_table_function_init_global_get_filter(self.raw, index as idx_t) -> Expression::from_raw),
        )
    }

    /// Whether the filter is optional: it holds for every row the query keeps, but need not be applied.
    pub fn filter_is_optional(&self, index: usize) -> Result<bool> {
        let mut optional = false;
        api_call!(duckdb_v2_table_function_init_global_filter_is_optional(self.raw, index as idx_t, &mut optional));
        Ok(optional)
    }

    /// Whether the scan produces rows only (`count(*)`): the engine asked for none of the function's columns, and the
    /// one column of [`column_indexes`](Self::column_indexes) - the first declared column - only carries the rows. The
    /// exec callback then sizes the chunk's first vector without writing values; the function may skip decoding every
    /// column. An addition of this port: a host without the entry reports `Error::missing_entry`.
    pub fn is_row_only(&self) -> Result<bool> {
        let mut row_only = false;
        api_call!(duckdb_v2_table_function_init_global_is_row_only(self.raw, &mut row_only));
        Ok(row_only)
    }

    /// What identifies the filter across the files of a multi-file scan: the position of its column in the
    /// output of the scan the function is part of (the filter's column itself outside a multi-file scan).
    pub fn filter_global_index(&self, index: usize) -> Result<usize> {
        let mut global_index: idx_t = 0;
        api_call!(duckdb_v2_table_function_init_global_get_filter_global_index(
            self.raw,
            index as idx_t,
            &mut global_index
        ));
        Ok(global_index as usize)
    }

    /// Declares that the function applies the filter itself; the engine evaluates the others.
    pub fn accept_filter(&self, index: usize) -> Result<()> {
        api_call!(duckdb_v2_table_function_init_global_accept_filter(self.raw, index as idx_t));
        Ok(())
    }

    /// An owned copy of the pushed filter at `index`, for the engine to evaluate on the values the function read
    /// (`TableFilter::select`); a function that evaluates a filter this way accepts it.
    pub fn table_filter(&self, index: usize) -> Result<TableFilter> {
        let mut raw = std::ptr::null_mut();
        api_call!(duckdb_v2_table_function_init_global_get_table_filter(self.raw, index as idx_t, &mut raw));
        Ok(TableFilter { raw })
    }

    extract_getters!(
        duckdb_v2_table_function_init_global_get_column_extract_depth,
        duckdb_v2_table_function_init_global_get_column_extract_field,
        duckdb_v2_table_function_init_global_get_column_extract_type
    );
}

/// An owned copy of a table filter pushed into a scan, evaluated by the engine on the values the function read.
pub struct TableFilter {
    raw: duckdb_v2_table_filter_handle,
}

// SAFETY(protocol L8.I8): (T5, L8.I1) an owned `duckdb_v2_table_filter_handle` (`duckdb_v2.h`: "a copy of a table
// filter the engine pushed into a scan", evaluated with `table_filter_select`); the one entry the wrapper calls,
// `select`, runs under the filter's own lock in the engine (the filter copy carries a mutex), so concurrent
// `select` calls through `&self` serialize there and the copy moves between
// threads as a whole.
unsafe impl Send for TableFilter {}
unsafe impl Sync for TableFilter {}

impl TableFilter {
    pub fn as_raw(&self) -> duckdb_v2_table_filter_handle {
        self.raw
    }

    /// The rows of `sel` (or the first `count` rows, without a selection) of `vector` - the values of the filtered
    /// column - that pass the filter, as the engine evaluates it under `context`.
    pub fn select(
        &self,
        context: &Context,
        vector: &Vector,
        count: usize,
        sel: Option<&[idx_t]>,
    ) -> Result<Vec<idx_t>> {
        let capacity = sel.map_or(count, |sel| sel.len());
        let mut out = vec![0 as idx_t; capacity.max(1)];
        let mut out_count: idx_t = 0;
        api_call!(duckdb_v2_table_filter_select(
            context.as_raw(),
            self.raw,
            vector.as_raw(),
            count as idx_t,
            sel.map_or(std::ptr::null(), |sel| sel.as_ptr()),
            sel.map_or(0, |sel| sel.len()) as idx_t,
            out.as_mut_ptr(),
            &mut out_count
        ));
        out.truncate(out_count as usize);
        Ok(out)
    }

    /// A copy of the filter with an evaluation state of its own (`duckdb_v2_table_filter_copy`): one per thread (scan
    /// state), like the engine's `TableFilterState`, so that the threads of a scan do not serialize on one handle.
    /// `None` when the host has no such entry (the threads then share the handle).
    pub fn try_clone(&self) -> Result<Option<TableFilter>> {
        if api().duckdb_v2_table_filter_copy.is_none() {
            return Ok(None);
        }
        let mut raw: duckdb_v2_table_filter_handle = std::ptr::null_mut();
        api_call!(duckdb_v2_table_filter_copy(self.raw, &mut raw));
        Ok(Some(TableFilter { raw }))
    }
}

impl Drop for TableFilter {
    fn drop(&mut self) {
        if let Some(destroy) = api().duckdb_v2_table_filter_destroy {
            // SAFETY(ffi L8.I1): the handle was returned by the engine and is destroyed once
            unsafe { destroy(&mut self.raw) };
        }
    }
}

/// The supports-pushdown-extract callback's info.
#[derive(Clone, Copy)]
pub struct TablePushdownExtractInfo {
    raw: duckdb_v2_table_function_pushdown_extract_info_handle,
}

impl TablePushdownExtractInfo {
    data_getter!(/// The function's user data.
        user_data, duckdb_v2_table_function_pushdown_extract_get_user_data);
    data_getter!(/// The bind data of the call.
        bind_data, duckdb_v2_table_function_pushdown_extract_get_bind_data);

    /// The declared column (a STRUCT / VARIANT, or a type nesting them) the optimizer asks about.
    pub fn column_index(&self) -> Result<usize> {
        let mut index: idx_t = 0;
        api_call!(duckdb_v2_table_function_pushdown_extract_get_column_index(self.raw, &mut index));
        Ok(index as usize)
    }

    /// Answers whether the scan extracts nested fields of the column itself (the default is false).
    pub fn set_supported(&self, supported: bool) -> Result<()> {
        api_call!(duckdb_v2_table_function_pushdown_extract_set_supported(self.raw, supported));
        Ok(())
    }
}

/// The local init callback's info.
#[derive(Clone, Copy)]
pub struct TableInitLocalInfo {
    raw: duckdb_v2_table_function_init_local_info_handle,
}

impl TableInitLocalInfo {
    data_getter!(/// The function's user data.
        user_data, duckdb_v2_table_function_init_local_get_user_data);
    data_getter!(/// The bind data of the call.
        bind_data, duckdb_v2_table_function_init_local_get_bind_data);
    data_getter!(/// The global state of the scan.
        global_state, duckdb_v2_table_function_init_local_get_global_state);
    column_indexes_getter!(
        duckdb_v2_table_function_init_local_get_column_count,
        duckdb_v2_table_function_init_local_get_column_index
    );

    pub fn set_local_state<T: 'static>(&self, state: T) -> Result<()> {
        let mut opaque = Opaque::new(state);
        api_call!(duckdb_v2_table_function_init_local_set_local_state(self.raw, &mut opaque));
        Ok(())
    }

    extract_getters!(
        duckdb_v2_table_function_init_local_get_column_extract_depth,
        duckdb_v2_table_function_init_local_get_column_extract_field,
        duckdb_v2_table_function_init_local_get_column_extract_type
    );
}

/// The exec callback's info.
pub struct TableExecInfo<'cb> {
    raw: duckdb_v2_table_function_exec_info_handle,
    /// The exclusive slots of this callback ([`CallbackSlots`]).
    slots: CallbackSlots<'cb>,
}

impl<'cb> TableExecInfo<'cb> {
    data_getter!(/// The function's user data.
        user_data, duckdb_v2_table_function_exec_get_user_data);
    data_getter!(/// The bind data of the call.
        bind_data, duckdb_v2_table_function_exec_get_bind_data);
    data_getter!(/// The global state of the scan.
        global_state, duckdb_v2_table_function_exec_get_global_state);
    exclusive_getter!(/// This thread's local state.
        /// (`duckdb_v2.h`: "associated with the executing thread for the duration of the read ... without synchronization"); handed out once per callback as `&mut` for the callback's duration.
        local_state, duckdb_v2_table_function_exec_get_local_state, LOCAL_STATE, "local_state");
    column_indexes_getter!(
        duckdb_v2_table_function_exec_get_column_count,
        duckdb_v2_table_function_exec_get_column_index
    );

    /// The chunk to fill (borrowed): write the columns and set its size; size 0 ends this thread's scan.
    #[inline]
    pub fn output(&self) -> Result<DataChunk> {
        Ok(api_call_out!(duckdb_v2_table_function_exec_get_output_chunk(self.raw) -> DataChunk::borrowed))
    }

    /// Logs an event of the scan under `PhysicalOperator`, attributed to the operator running it
    /// (`duckdb_logs_parsed('PhysicalOperator')`): `class` names the reporting component, `event` the
    /// event, `details` its key-value facts.
    pub fn log_operator_event(
        &self,
        context: &Context,
        class: &str,
        event: &str,
        details: &[(&str, &str)],
    ) -> Result<()> {
        let class = str_arg(class);
        let event = str_arg(event);
        let keys: Vec<duckdb_v2_str> = details.iter().map(|(key, _)| str_arg(key)).collect();
        let values: Vec<duckdb_v2_str> = details.iter().map(|(_, value)| str_arg(value)).collect();
        api_call!(duckdb_v2_table_function_exec_log_operator_event(
            self.raw,
            context.as_raw(),
            &class,
            &event,
            if keys.is_empty() { std::ptr::null() } else { keys.as_ptr() },
            if values.is_empty() { std::ptr::null() } else { values.as_ptr() },
            keys.len() as idx_t
        ));
        Ok(())
    }
}

/// The in-out callback's info: the input chunk (the rows of the arguments), the output chunk and the states.
pub struct TableInOutInfo<'cb> {
    raw: duckdb_v2_table_function_in_out_info_handle,
    /// The exclusive slots of this callback ([`CallbackSlots`]).
    slots: CallbackSlots<'cb>,
}

impl<'cb> TableInOutInfo<'cb> {
    data_getter!(/// The function's user data.
        user_data, duckdb_v2_table_function_in_out_get_user_data);
    data_getter!(/// The bind data of the call.
        bind_data, duckdb_v2_table_function_in_out_get_bind_data);
    data_getter!(/// The global state.
        global_state, duckdb_v2_table_function_in_out_get_global_state);
    exclusive_getter!(/// This thread's local state.
        /// (`duckdb_v2.h`: "associated with the executing thread for the duration of the read ... without synchronization"); handed out once per callback as `&mut` for the callback's duration.
        local_state, duckdb_v2_table_function_in_out_get_local_state, LOCAL_STATE, "local_state");

    /// The input chunk (borrowed): one row per evaluation of the arguments.
    pub fn input(&self) -> Result<DataChunk> {
        Ok(api_call_out!(duckdb_v2_table_function_in_out_get_input_chunk(self.raw) -> DataChunk::borrowed))
    }

    /// The chunk to fill (borrowed): write the projected columns and set its size.
    #[inline]
    pub fn output(&self) -> Result<DataChunk> {
        Ok(api_call_out!(duckdb_v2_table_function_in_out_get_output_chunk(self.raw) -> DataChunk::borrowed))
    }

    /// The number of projected output columns.
    pub fn column_count(&self) -> Result<usize> {
        let mut count: idx_t = 0;
        api_call!(duckdb_v2_table_function_in_out_get_column_count(self.raw, &mut count));
        Ok(count as usize)
    }

    /// The declared column the output column at `index` holds.
    pub fn column_index(&self, index: usize) -> Result<usize> {
        let mut column: idx_t = 0;
        api_call!(duckdb_v2_table_function_in_out_get_column_index(self.raw, index as idx_t, &mut column));
        Ok(column as usize)
    }

    /// What the callback reports about its output (the trampoline sets the trait's return value).
    pub fn set_result(&self, result: OperatorResult) -> Result<()> {
        api_call!(duckdb_v2_table_function_in_out_set_result(self.raw, result));
        Ok(())
    }
}

/// The get-metrics callback's info.
#[derive(Clone, Copy)]
pub struct TableGetMetricsInfo {
    raw: duckdb_v2_table_function_get_metrics_info_handle,
}

impl TableGetMetricsInfo {
    data_getter!(/// The function's user data.
        user_data, duckdb_v2_table_function_get_metrics_get_user_data);
    data_getter!(/// The bind data of the call.
        bind_data, duckdb_v2_table_function_get_metrics_get_bind_data);
    data_getter!(/// The global state of the scan.
        global_state, duckdb_v2_table_function_get_metrics_get_global_state);
    data_getter!(/// The local state of the thread whose metrics are collected (none for the scan as a whole): a read
        /// of a state another callback owns.
        local_state, duckdb_v2_table_function_get_metrics_get_local_state);

    /// Reports row groups read since the metrics were last collected (summed by the profiler).
    pub fn add_row_groups_scanned(&self, count: u64) -> Result<()> {
        api_call!(duckdb_v2_table_function_get_metrics_add_row_groups_scanned(self.raw, count as idx_t));
        Ok(())
    }

    /// Reports the number of row groups the scan considers in total (as-is).
    pub fn set_total_row_groups_to_scan(&self, count: u64) -> Result<()> {
        api_call!(duckdb_v2_table_function_get_metrics_set_total_row_groups_to_scan(self.raw, count as idx_t));
        Ok(())
    }

    /// Reports bytes scanned since the metrics were last collected (summed by the profiler): the compressed size of
    /// the column chunks scanned, however many bytes were read from storage to scan them.
    pub fn add_bytes_scanned(&self, count: u64) -> Result<()> {
        api_call!(duckdb_v2_table_function_get_metrics_add_bytes_scanned(self.raw, count as idx_t));
        Ok(())
    }
}

/// The statistics callback's info: which column is asked about, and where its statistics go.
#[derive(Clone, Copy)]
pub struct TableStatisticsInfo {
    raw: duckdb_v2_table_function_statistics_info_handle,
}

impl TableStatisticsInfo {
    data_getter!(/// The function's user data.
        user_data, duckdb_v2_table_function_statistics_get_user_data);
    data_getter!(/// The bind data of the call.
        bind_data, duckdb_v2_table_function_statistics_get_bind_data);

    /// The declared column (by index) or virtual column (by id) whose statistics are asked for.
    pub fn column_index(&self) -> Result<u64> {
        let mut index: idx_t = 0;
        api_call!(duckdb_v2_table_function_statistics_get_column_index(self.raw, &mut index));
        Ok(index)
    }

    /// The type of that column: the statistics reported must be of it.
    pub fn column_type(&self) -> Result<LogicalType> {
        Ok(api_call_out!(duckdb_v2_table_function_statistics_get_column_type(self.raw) -> LogicalType::from_raw))
    }

    /// Reports the statistics of the column (copied; the caller keeps ownership).
    pub fn set_result(&self, statistics: &Statistics) -> Result<()> {
        api_call!(duckdb_v2_table_function_statistics_set_result(self.raw, statistics.as_raw()));
        Ok(())
    }
}

/// The stats callback's info (borrowed, valid only for the callback duration).
///
/// The column statistics start out unknown, with every kind of value possible, and describe every value the scan
/// produces for the column. A callback that leaves them unchanged tells the engine nothing.
#[derive(Clone, Copy)]
pub struct TableStatsInfo {
    raw: duckdb_v2_table_function_stats_info_handle,
}

impl TableStatsInfo {
    data_getter!(/// The function's user data.
        user_data, duckdb_v2_table_function_stats_get_user_data);
    data_getter!(/// The bind data of the call.
        bind_data, duckdb_v2_table_function_stats_get_bind_data);

    /// Which declared column (in [`TableBindInfo::add_result_column`] order) the statistics describe.
    pub fn column_index(&self) -> Result<usize> {
        let mut index: idx_t = 0;
        api_call!(duckdb_v2_table_function_stats_get_column_index(self.raw, &mut index));
        Ok(index as usize)
    }

    /// The statistics of the column, for the callback to narrow.
    pub fn result_stats(&self) -> Result<Stats> {
        let mut raw: duckdb_v2_stats_handle = std::ptr::null_mut();
        api_call!(duckdb_v2_table_function_stats_get_result_stats(self.raw, &mut raw));
        // SAFETY(ffi L8.I1): the engine lends a live stats handle for the duration of the callback.
        Ok(unsafe { Stats::from_raw(raw) })
    }
}

/// The progress callback's info.
#[derive(Clone, Copy)]
pub struct TableProgressInfo {
    raw: duckdb_v2_table_function_progress_info_handle,
}

impl TableProgressInfo {
    data_getter!(/// The function's user data.
        user_data, duckdb_v2_table_function_progress_get_user_data);
    data_getter!(/// The bind data of the call.
        bind_data, duckdb_v2_table_function_progress_get_bind_data);
    data_getter!(/// The global state of the scan (read concurrently with the scan).
        global_state, duckdb_v2_table_function_progress_get_global_state);

    /// 0.0 to 1.0.
    pub fn set_progress(&self, progress: f64) -> Result<()> {
        api_call!(duckdb_v2_table_function_progress_set_progress(self.raw, progress));
        Ok(())
    }
}

/// The claim-batch callback's info.
pub struct TableClaimBatchInfo<'cb> {
    raw: duckdb_v2_table_function_claim_batch_info_handle,
    /// The exclusive slots of this callback ([`CallbackSlots`]).
    slots: CallbackSlots<'cb>,
}

impl<'cb> TableClaimBatchInfo<'cb> {
    data_getter!(/// The function's user data.
        user_data, duckdb_v2_table_function_claim_batch_get_user_data);
    data_getter!(/// The bind data of the call.
        bind_data, duckdb_v2_table_function_claim_batch_get_bind_data);
    data_getter!(/// The global state of the scan.
        global_state, duckdb_v2_table_function_claim_batch_get_global_state);
    exclusive_getter!(/// This thread's local state.
        /// (`duckdb_v2.h`: "associated with the executing thread for the duration of the read ... without synchronization"); handed out once per callback as `&mut` for the callback's duration.
        local_state, duckdb_v2_table_function_claim_batch_get_local_state, LOCAL_STATE, "local_state");

    /// Whether a batch was claimed (false: nothing left to scan).
    pub fn set_claimed(&self, claimed: bool) -> Result<()> {
        api_call!(duckdb_v2_table_function_claim_batch_set_claimed(self.raw, claimed));
        Ok(())
    }
}

/// The schedule-io callback's info.
pub struct TableScheduleIoInfo<'cb> {
    raw: duckdb_v2_table_function_schedule_io_info_handle,
    /// The exclusive slots of this callback ([`CallbackSlots`]).
    slots: CallbackSlots<'cb>,
}

impl<'cb> TableScheduleIoInfo<'cb> {
    data_getter!(/// The function's user data.
        user_data, duckdb_v2_table_function_schedule_io_get_user_data);
    data_getter!(/// The bind data of the call.
        bind_data, duckdb_v2_table_function_schedule_io_get_bind_data);
    data_getter!(/// The global state of the scan.
        global_state, duckdb_v2_table_function_schedule_io_get_global_state);
    exclusive_getter!(/// The local state the batch was claimed for.
        /// (`duckdb_v2.h`: "associated with the executing thread for the duration of the read ... without synchronization"); handed out once per callback as `&mut` for the callback's duration.
        local_state, duckdb_v2_table_function_schedule_io_get_local_state, LOCAL_STATE, "local_state");

    /// Adds an I/O task: `task(task_data)` runs on an I/O thread of the engine before the batch is scanned,
    /// `destroy(task_data)` afterwards (or when the task is dropped without running); `io_size` is the number of
    /// bytes the task reads, when known.
    #[expect(
        clippy::not_unsafe_ptr_arg_deref,
        reason = "`task_data` is passed through to the engine, which hands it back to `task` / `destroy`"
    )]
    pub fn add_task(
        &self,
        task: duckdb_v2_async_task_fn,
        task_data: *mut c_void,
        destroy: duckdb_v2_opaque_destroy_fn,
        io_size: idx_t,
    ) -> Result<()> {
        api_call!(duckdb_v2_table_function_schedule_io_add_task(self.raw, task, task_data, destroy, io_size));
        Ok(())
    }
}

/// The prepare-read-ahead callback's info: a file the read-ahead opened, its global state initialized.
pub struct TablePrepareReadAheadInfo {
    raw: duckdb_v2_table_function_prepare_read_ahead_info_handle,
}

impl TablePrepareReadAheadInfo {
    data_getter!(/// The function's user data.
        user_data, duckdb_v2_table_function_prepare_read_ahead_get_user_data);
    data_getter!(/// The bind data of the file's call.
        bind_data, duckdb_v2_table_function_prepare_read_ahead_get_bind_data);
    data_getter!(/// The global state of the file's scan (its batches are not claimed yet).
        global_state, duckdb_v2_table_function_prepare_read_ahead_get_global_state);
}

/// The partition-data callback's info.
pub struct TablePartitionDataInfo<'cb> {
    raw: duckdb_v2_table_function_partition_data_info_handle,
    /// The exclusive slots of this callback ([`CallbackSlots`]).
    slots: CallbackSlots<'cb>,
}

impl<'cb> TablePartitionDataInfo<'cb> {
    data_getter!(/// The function's user data.
        user_data, duckdb_v2_table_function_partition_data_get_user_data);
    data_getter!(/// The bind data of the call.
        bind_data, duckdb_v2_table_function_partition_data_get_bind_data);
    data_getter!(/// The global state of the scan.
        global_state, duckdb_v2_table_function_partition_data_get_global_state);
    exclusive_getter!(/// This thread's local state.
        /// (`duckdb_v2.h`: "associated with the executing thread for the duration of the read ... without synchronization"); handed out once per callback as `&mut` for the callback's duration.
        local_state, duckdb_v2_table_function_partition_data_get_local_state, LOCAL_STATE, "local_state");

    pub fn requires_batch_index(&self) -> Result<bool> {
        let mut required = false;
        api_call!(duckdb_v2_table_function_partition_data_requires_batch_index(self.raw, &mut required));
        Ok(required)
    }

    pub fn requires_partition_columns(&self) -> Result<bool> {
        let mut required = false;
        api_call!(duckdb_v2_table_function_partition_data_requires_partition_columns(self.raw, &mut required));
        Ok(required)
    }

    /// The declared column indexes of the partitioning columns.
    pub fn partition_columns(&self) -> Result<Vec<usize>> {
        let mut count: idx_t = 0;
        api_call!(duckdb_v2_table_function_partition_data_get_partition_column_count(self.raw, &mut count));
        let mut columns = Vec::with_capacity(count as usize);
        for i in 0..count {
            let mut column: idx_t = 0;
            api_call!(duckdb_v2_table_function_partition_data_get_partition_column_index(self.raw, i, &mut column));
            columns.push(column as usize);
        }
        Ok(columns)
    }

    /// Required on every call.
    pub fn set_batch_index(&self, batch_index: usize) -> Result<()> {
        api_call!(duckdb_v2_table_function_partition_data_set_batch_index(self.raw, batch_index as idx_t));
        Ok(())
    }

    /// The constant value of partitioning column `index` (of [`Self::partition_columns`]) in this batch.
    pub fn set_partition_value(&self, index: usize, value: &Value) -> Result<()> {
        api_call!(duckdb_v2_table_function_partition_data_set_partition_value(
            self.raw,
            index as idx_t,
            value.as_raw()
        ));
        Ok(())
    }
}

/// The partitioning callback's info.
#[derive(Clone, Copy)]
pub struct TablePartitioningInfo {
    raw: duckdb_v2_table_function_partitioning_info_handle,
}

impl TablePartitioningInfo {
    data_getter!(/// The function's user data.
        user_data, duckdb_v2_table_function_partitioning_get_user_data);
    data_getter!(/// The bind data of the call.
        bind_data, duckdb_v2_table_function_partitioning_get_bind_data);

    /// The declared column indexes the planner asks about.
    pub fn partition_columns(&self) -> Result<Vec<usize>> {
        let mut count: idx_t = 0;
        api_call!(duckdb_v2_table_function_partitioning_get_partition_column_count(self.raw, &mut count));
        let mut columns = Vec::with_capacity(count as usize);
        for i in 0..count {
            let mut column: idx_t = 0;
            api_call!(duckdb_v2_table_function_partitioning_get_partition_column_index(self.raw, i, &mut column));
            columns.push(column as usize);
        }
        Ok(columns)
    }

    pub fn set_partition_info(&self, info: TablePartitionInfo) -> Result<()> {
        api_call!(duckdb_v2_table_function_partitioning_set_partition_info(self.raw, info));
        Ok(())
    }
}

/// The filter-pushdown callback's info.
#[derive(Clone, Copy)]
pub struct TableFilterPushdownInfo {
    raw: duckdb_v2_table_function_filter_pushdown_info_handle,
}

impl TableFilterPushdownInfo {
    data_getter!(/// The function's user data.
        user_data, duckdb_v2_table_function_filter_pushdown_get_user_data);
    data_getter!(/// The bind data of the call.
        bind_data, duckdb_v2_table_function_filter_pushdown_get_bind_data);

    pub fn filter_count(&self) -> Result<usize> {
        let mut count: idx_t = 0;
        api_call!(duckdb_v2_table_function_filter_pushdown_get_filter_count(self.raw, &mut count));
        Ok(count as usize)
    }

    /// Filter `index`, over the scanned columns ([`Self::column_indexes`] maps column references).
    pub fn filter(&self, index: usize) -> Result<Expression> {
        Ok(
            api_call_out!(duckdb_v2_table_function_filter_pushdown_get_filter(self.raw, index as idx_t) -> Expression::from_raw),
        )
    }

    /// The declared column index of every scanned column.
    pub fn column_indexes(&self) -> Result<Vec<usize>> {
        let mut count: idx_t = 0;
        api_call!(duckdb_v2_table_function_filter_pushdown_get_column_count(self.raw, &mut count));
        let mut columns = Vec::with_capacity(count as usize);
        for i in 0..count {
            let mut column: idx_t = 0;
            api_call!(duckdb_v2_table_function_filter_pushdown_get_column_index(self.raw, i, &mut column));
            columns.push(column as usize);
        }
        Ok(columns)
    }

    /// Takes over filter `index`: the engine no longer evaluates it.
    pub fn accept(&self, index: usize) -> Result<()> {
        api_call!(duckdb_v2_table_function_filter_pushdown_accept(self.raw, index as idx_t));
        Ok(())
    }
}

/// The get-bind-info callback's info.
#[derive(Clone, Copy)]
pub struct TableGetBindInfoInfo {
    raw: duckdb_v2_table_function_get_bind_info_info_handle,
}

impl TableGetBindInfoInfo {
    data_getter!(/// The function's user data.
        user_data, duckdb_v2_table_function_get_bind_info_get_user_data);
    data_getter!(/// The bind data of the call.
        bind_data, duckdb_v2_table_function_get_bind_info_get_bind_data);

    /// The identifier (INTEGER field id or VARCHAR name) of a (nested) column.
    pub fn set_column_identifier(&self, column: usize, child_path: &[usize], identifier: &Value) -> Result<()> {
        let path: Vec<idx_t> = child_path.iter().map(|&i| i as idx_t).collect();
        api_call!(duckdb_v2_table_function_get_bind_info_set_column_identifier(
            self.raw,
            column as idx_t,
            if path.is_empty() { std::ptr::null() } else { path.as_ptr() },
            path.len() as idx_t,
            identifier.as_raw()
        ));
        Ok(())
    }

    pub fn set_option(&self, key: &str, value: &Value) -> Result<()> {
        let mut key = str_arg(key);
        api_call!(duckdb_v2_table_function_get_bind_info_set_option(self.raw, &mut key, value.as_raw()));
        Ok(())
    }
}

/// A multi-file function under construction: wraps a registered single-file table function (one
/// positional VARCHAR/ANY parameter: the file) with globs, file lists, `union_by_name`, hive
/// partitioning and the `filename` / `file_row_number` / `file_index` virtual columns.
pub struct MultiFileFunction {
    raw: duckdb_v2_multi_file_function_handle,
}

impl MultiFileFunction {
    pub fn new(factory: Factory) -> Result<MultiFileFunction> {
        let mut raw = std::ptr::null_mut();
        api_call!(duckdb_v2_multi_file_function_create(factory.as_raw(), &mut raw));
        Ok(MultiFileFunction { raw })
    }

    pub fn as_raw(&self) -> duckdb_v2_multi_file_function_handle {
        self.raw
    }

    pub fn set_name(&self, name: &str) -> Result<&Self> {
        let mut name = str_arg(name);
        api_call!(duckdb_v2_multi_file_function_set_name(self.raw, &mut name));
        Ok(self)
    }

    /// The registered table function that reads one file.
    pub fn set_single_file_function(&self, name: &str) -> Result<&Self> {
        let mut name = str_arg(name);
        api_call!(duckdb_v2_multi_file_function_set_single_file_function(self.raw, &mut name));
        Ok(self)
    }

    /// How the files are called in error messages (e.g. "Parquet").
    pub fn set_reader_type(&self, reader_type: &str) -> Result<&Self> {
        let mut reader_type = str_arg(reader_type);
        api_call!(duckdb_v2_multi_file_function_set_reader_type(self.raw, &mut reader_type));
        Ok(self)
    }

    /// The extension (without dot) a directory glob falls back to, e.g. "parquet".
    pub fn set_file_extension(&self, extension: &str) -> Result<&Self> {
        let mut extension = str_arg(extension);
        api_call!(duckdb_v2_multi_file_function_set_file_extension(self.raw, &mut extension));
        Ok(self)
    }

    /// Declares an option of the multi-file function itself, next to the forwarded named parameters of the
    /// single-file function and the options every multi-file function accepts - e.g. `bound_schema`, which the
    /// multi-file framework parses but only declares for the readers that ask for it.
    pub fn add_option(&self, name: &str, type_: &LogicalType) -> Result<&Self> {
        let mut name = str_arg(name);
        api_call!(duckdb_v2_multi_file_function_add_option(self.raw, &mut name, type_.as_raw()));
        Ok(self)
    }

    /// Whether one local state of the wrapped function reads several files in turn (what it learned
    /// reading a file carries over to the next).
    pub fn set_reuses_local_state(&self, reuses_local_state: bool) -> Result<&Self> {
        api_call!(duckdb_v2_multi_file_function_set_reuses_local_state(self.raw, reuses_local_state));
        Ok(self)
    }

    /// Whether the wrapped single-file function counts the bytes it scans (`TableGetMetricsInfo::add_bytes_scanned`),
    /// so that the scan reports `operator.bytes_scanned` rather than leaving it out.
    pub fn set_reports_bytes_scanned(&self, reports_bytes_scanned: bool) -> Result<&Self> {
        api_call!(duckdb_v2_multi_file_function_set_reports_bytes_scanned(self.raw, reports_bytes_scanned));
        Ok(self)
    }

    /// Sets the "partition stats" callback: the row groups of a scan over several files, from what the function
    /// cached about them (a scan over one file asks the single-file function's partition stats).
    pub fn set_partition_stats<F: MultiFilePartitionStats>(&self) -> Result<&Self> {
        api_call!(duckdb_v2_multi_file_function_set_partition_stats_callback(
            self.raw,
            Some(multi_file_partition_stats_trampoline::<F>)
        ));
        Ok(self)
    }
}

impl MultiFileFunction {
    /// `TableFunctionMultiFileSettings::maximum_sample_files`: how many files the wrapper samples (binds) to
    /// determine the schema of a scan (newer engines only; see [`MultiFileFunction::sampling_available`]).
    pub fn set_maximum_sample_files(&self, count: usize) -> Result<&Self> {
        api_call!(duckdb_v2_multi_file_function_set_maximum_sample_files(self.raw, count as idx_t));
        Ok(self)
    }

    /// The named parameter of the single-file function that sets the number of sampled files.
    pub fn set_sample_files_parameter(&self, parameter: &str) -> Result<&Self> {
        let mut parameter = str_arg(parameter);
        api_call!(duckdb_v2_multi_file_function_set_sample_files_parameter(self.raw, &mut parameter));
        Ok(self)
    }

    /// Whether the loading engine takes the schema-sampling settings.
    pub fn sampling_available() -> bool {
        let api = api::api();
        api.duckdb_v2_multi_file_function_set_maximum_sample_files.is_some()
            && api.duckdb_v2_multi_file_function_set_sample_files_parameter.is_some()
    }

    /// Sets the "combine schema" callback (`table_function_combine_schema_t`): combines the schemas of the sampled
    /// files (their bind data) into the schema of the scan. newer engines only; see
    /// [`MultiFileFunction::combine_schema_available`].
    pub fn set_combine_schema<F: MultiFileCombineSchema>(&self) -> Result<&Self> {
        api_call!(duckdb_v2_multi_file_function_set_combine_schema_callback(
            self.raw,
            Some(multi_file_combine_schema_trampoline::<F>)
        ));
        Ok(self)
    }

    pub fn combine_schema_available() -> bool {
        api::api().duckdb_v2_multi_file_function_set_combine_schema_callback.is_some()
    }
}

/// Combines the schemas of the sampled files of a multi-file scan into the schema every file is then read with.
pub trait MultiFileCombineSchema {
    fn combine_schema(info: &MultiFileCombineSchemaInfo, context: &Context) -> Result<()>;
}
trampoline!(
    multi_file_combine_schema_trampoline,
    MultiFileCombineSchema::combine_schema,
    MultiFileCombineSchemaInfo,
    duckdb_v2_multi_file_combine_schema_info_handle
);

/// The combine-schema callback's info (`TableFunctionCombineSchemaInput`): the bind data of the sampled files, and
/// the combined schema and bind data the callback may set (leaving them unset keeps every file's own bind).
#[derive(Clone, Copy)]
pub struct MultiFileCombineSchemaInfo {
    raw: duckdb_v2_multi_file_combine_schema_info_handle,
}

impl MultiFileCombineSchemaInfo {
    data_getter!(/// The single-file function's user data.
        user_data, duckdb_v2_multi_file_combine_schema_get_user_data);

    /// The number of sampled files (whose bind data [`Self::bind_data`] returns).
    pub fn bind_data_count(&self) -> Result<usize> {
        let mut count: idx_t = 0;
        api_call!(duckdb_v2_multi_file_combine_schema_get_bind_data_count(self.raw, &mut count));
        Ok(count as usize)
    }

    /// The bind data of sampled file `index`, as `T`.
    pub fn bind_data<T>(&self, index: usize) -> Result<Option<&T>> {
        let mut data: *mut c_void = std::ptr::null_mut();
        api_call!(duckdb_v2_multi_file_combine_schema_get_bind_data(self.raw, index as idx_t, &mut data));
        // SAFETY(ffi L8.I1): the slot holds what a bind of the single-file function stored, which the caller names as
        // `T`.
        Ok(unsafe { Opaque::get::<T>(data) })
    }

    /// Whether the scan unions the files' schemas by name.
    pub fn union_by_name(&self) -> Result<bool> {
        let mut union_by_name = false;
        api_call!(duckdb_v2_multi_file_combine_schema_get_union_by_name(self.raw, &mut union_by_name));
        Ok(union_by_name)
    }

    /// Declares a column of the combined schema.
    pub fn add_result_column(&self, name: &str, type_: &LogicalType) -> Result<&Self> {
        let name = str_arg(name);
        api_call!(duckdb_v2_multi_file_combine_schema_add_result_column(self.raw, &name, type_.as_raw()));
        Ok(self)
    }

    /// The bind data every file is then read with.
    pub fn set_bind_data<T: 'static>(&self, data: T) -> Result<()> {
        let mut opaque = Opaque::new(data);
        api_call!(duckdb_v2_multi_file_combine_schema_set_bind_data(self.raw, &mut opaque));
        Ok(())
    }
}

/// Reports the row groups of a scan over several files without opening them, or none.
pub trait MultiFilePartitionStats {
    fn partition_stats(info: &MultiFilePartitionStatsInfo, context: &Context) -> Result<()>;
}
trampoline!(
    multi_file_partition_stats_trampoline,
    MultiFilePartitionStats::partition_stats,
    MultiFilePartitionStatsInfo,
    duckdb_v2_multi_file_partition_stats_info_handle
);

/// The multi-file partition-stats callback's info.
#[derive(Clone, Copy)]
pub struct MultiFilePartitionStatsInfo {
    raw: duckdb_v2_multi_file_partition_stats_info_handle,
}

impl MultiFilePartitionStatsInfo {
    data_getter!(/// The single-file function's user data.
        user_data, duckdb_v2_multi_file_partition_stats_get_user_data);
    data_getter!(/// The bind data the scan's schema was taken from (`None` when no file was bound).
        bind_data, duckdb_v2_multi_file_partition_stats_get_bind_data);

    /// The number of files of the scan.
    pub fn file_count(&self) -> Result<usize> {
        let mut count: idx_t = 0;
        api_call!(duckdb_v2_multi_file_partition_stats_get_file_count(self.raw, &mut count));
        Ok(count as usize)
    }

    /// A file of the scan as the value the single-file function is bound with: a VARCHAR path, or a STRUCT with
    /// the path in `filename` and the file's open options (`last_modified`, `etag`, ...) in the other fields.
    pub fn file(&self, index: usize) -> Result<Value> {
        Ok(api_call_out!(duckdb_v2_multi_file_partition_stats_get_file(self.raw, index as idx_t) -> Value::from_raw))
    }

    /// Adds a row group of one of the files: `row_start` within its file, `count` rows (exact or approximate).
    pub fn add_partition(&self, row_start: idx_t, count: idx_t, count_is_exact: bool) -> Result<()> {
        api_call!(duckdb_v2_multi_file_partition_stats_add_partition(self.raw, row_start, count, count_is_exact));
        Ok(())
    }
}

impl Drop for MultiFileFunction {
    fn drop(&mut self) {
        api_destroy!(duckdb_v2_multi_file_function_destroy(&mut self.raw));
    }
}

/// The partition stats callback's info: where the partitions go.
#[derive(Clone, Copy)]
pub struct TablePartitionStatsInfo {
    raw: duckdb_v2_table_function_partition_stats_info_handle,
}

impl TablePartitionStatsInfo {
    data_getter!(/// The function's user data.
        user_data, duckdb_v2_table_function_partition_stats_get_user_data);
    data_getter!(/// The bind data of the call.
        bind_data, duckdb_v2_table_function_partition_stats_get_bind_data);

    /// Reports the next partition: its first row, its row count (exact or estimated) and whether the partition
    /// column statistics callback may be asked about its columns. Partitions are numbered from 0 in this order.
    pub fn add_partition(
        &self,
        row_start: u64,
        count: u64,
        count_is_exact: bool,
        has_column_statistics: bool,
    ) -> Result<()> {
        api_call!(duckdb_v2_table_function_partition_stats_add_partition(
            self.raw,
            row_start,
            count,
            count_is_exact,
            has_column_statistics
        ));
        Ok(())
    }
}

/// The partition column statistics callback's info: which partition and column are asked about.
#[derive(Clone, Copy)]
pub struct TablePartitionColumnStatisticsInfo {
    raw: duckdb_v2_table_function_partition_column_statistics_info_handle,
}

impl TablePartitionColumnStatisticsInfo {
    data_getter!(/// The function's user data.
        user_data, duckdb_v2_table_function_partition_column_statistics_get_user_data);
    data_getter!(/// The bind data of the call.
        bind_data, duckdb_v2_table_function_partition_column_statistics_get_bind_data);

    /// The partition (as numbered by the partition stats callback).
    pub fn partition_index(&self) -> Result<u64> {
        let mut index: idx_t = 0;
        api_call!(duckdb_v2_table_function_partition_column_statistics_get_partition_index(self.raw, &mut index));
        Ok(index)
    }

    /// The declared column (by index) or virtual column (by id) whose statistics are asked for.
    pub fn column_index(&self) -> Result<u64> {
        let mut index: idx_t = 0;
        api_call!(duckdb_v2_table_function_partition_column_statistics_get_column_index(self.raw, &mut index));
        Ok(index)
    }

    /// The type of that column: the statistics reported must be of it.
    pub fn column_type(&self) -> Result<LogicalType> {
        Ok(
            api_call_out!(duckdb_v2_table_function_partition_column_statistics_get_column_type(self.raw) -> LogicalType::from_raw),
        )
    }

    /// Reports the statistics of the column within the partition (copied; the caller keeps ownership), and
    /// whether their minimum and maximum are values the partition holds.
    pub fn set_result(&self, statistics: &Statistics, min_max_is_exact: bool) -> Result<()> {
        api_call!(duckdb_v2_table_function_partition_column_statistics_set_result(
            self.raw,
            statistics.as_raw(),
            min_max_is_exact
        ));
        Ok(())
    }
}

/// The projection expression pushdown callback's info: the offered expression and its column.
#[derive(Clone, Copy)]
pub struct TableProjectionExpressionPushdownInfo {
    raw: duckdb_v2_table_function_projection_expression_pushdown_info_handle,
}

impl TableProjectionExpressionPushdownInfo {
    data_getter!(/// The function's user data.
        user_data, duckdb_v2_table_function_projection_expression_pushdown_get_user_data);
    data_getter!(/// The bind data of the call (the scan runs with it: an accepted expression is recorded in it through the
        /// bind data's own interior mutability - the slot is shared with every later callback).
        bind_data, duckdb_v2_table_function_projection_expression_pushdown_get_bind_data);

    /// The declared column the expression is over.
    pub fn column_index(&self) -> Result<u64> {
        let mut index: idx_t = 0;
        api_call!(duckdb_v2_table_function_projection_expression_pushdown_get_column_index(self.raw, &mut index));
        Ok(index)
    }

    /// The offered expression (borrowed for the duration of the callback).
    pub fn expression(&self) -> Result<Expression> {
        Ok(
            api_call_out!(duckdb_v2_table_function_projection_expression_pushdown_get_expression(self.raw) -> Expression::from_raw),
        )
    }

    /// Accepts the expression: the scan produces its result, of its return type, in place of the column.
    pub fn accept(&self) -> Result<()> {
        api_call!(duckdb_v2_table_function_projection_expression_pushdown_accept(self.raw));
        Ok(())
    }
}

/// One filter a scan state evaluates, as [`AdaptiveFilter::initialize`] orders it: what identifies it across the
/// files of a multi-file scan ([`TableInitGlobalInfo::filter_global_index`]) and the predicates it consists of
/// (ANDed; they decide its initial position and whether the order may be adapted at all).
#[derive(Clone, Copy)]
pub struct AdaptiveFilterEntry<'a> {
    pub identity: usize,
    pub predicates: &'a [Expression],
}

/// The order a scan state evaluates its filters in, learned from their measured runtime (the engine's
/// `AdaptiveFilter`): time the filtering of every vector with [`begin_filter`](Self::begin_filter) /
/// [`end_filter`](Self::end_filter), evaluating the filters in the order of [`permutation`](Self::permutation).
/// Belongs to one scan state: used by one thread at a time.
#[derive(Default)]
pub struct AdaptiveFilter {
    raw: duckdb_v2_adaptive_filter_handle,
    len: usize,
}

// SAFETY(protocol L8.I8): (T5, L8.I1) an owned `duckdb_v2_adaptive_filter_handle` (`duckdb_v2.h`: "A filter belongs to
// one scan state: it is used by one thread at a time"), so `Send` - it moves between threads with the scan state that
// owns it - and not `Sync`.
unsafe impl Send for AdaptiveFilter {}

impl AdaptiveFilter {
    /// A filter not initialized for any file yet.
    pub fn new() -> AdaptiveFilter {
        AdaptiveFilter::default()
    }

    /// Whether the engine offers adaptive filters (an older engine does not: the scan then evaluates its filters
    /// in their own order).
    pub fn available() -> bool {
        crate::api::api().duckdb_v2_adaptive_filter_initialize.is_some()
    }

    /// Initializes the filter for the file the scan state starts reading: the learned order is kept when the
    /// file has the same filters (by identity) as the previous one, else the order starts over. `file_path` names
    /// the file in the `AdaptiveFilter` log.
    pub fn initialize(
        &mut self,
        context: &Context,
        filters: &[AdaptiveFilterEntry<'_>],
        file_path: &str,
    ) -> Result<()> {
        let identities: Vec<idx_t> = filters.iter().map(|filter| filter.identity as idx_t).collect();
        let predicate_counts: Vec<idx_t> = filters.iter().map(|filter| filter.predicates.len() as idx_t).collect();
        let predicates: Vec<duckdb_v2_expression_handle> =
            filters.iter().flat_map(|filter| filter.predicates.iter().map(Expression::as_raw)).collect();
        let path = str_arg(file_path);
        api_call!(duckdb_v2_adaptive_filter_initialize(
            context.as_raw(),
            &mut self.raw,
            filters.len() as idx_t,
            identities.as_ptr(),
            predicates.as_ptr(),
            predicate_counts.as_ptr(),
            &path
        ));
        self.len = filters.len();
        Ok(())
    }

    /// Whether [`initialize`](Self::initialize) has run.
    pub fn is_initialized(&self) -> bool {
        !self.raw.is_null()
    }

    /// The number of filters the adaptive filter orders.
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// The order to evaluate the filters in, written into `order` (resized to the filter count): the index of
    /// the filter to evaluate first, second, ...
    pub fn permutation(&self, order: &mut Vec<usize>) -> Result<()> {
        let mut buffer: Vec<idx_t> = vec![0; self.len];
        api_call!(duckdb_v2_adaptive_filter_get_permutation(self.raw, buffer.as_mut_ptr(), self.len as idx_t));
        order.clear();
        order.extend(buffer.iter().map(|index| *index as usize));
        Ok(())
    }

    /// Starts timing the filtering of one vector.
    pub fn begin_filter(&self) -> Result<()> {
        api_call!(duckdb_v2_adaptive_filter_begin_filter(self.raw));
        Ok(())
    }

    /// Ends the timing: the runtime adapts the order.
    pub fn end_filter(&self) -> Result<()> {
        api_call!(duckdb_v2_adaptive_filter_end_filter(self.raw));
        Ok(())
    }

    pub fn as_raw(&self) -> duckdb_v2_adaptive_filter_handle {
        self.raw
    }
}

impl Drop for AdaptiveFilter {
    fn drop(&mut self) {
        api_destroy!(duckdb_v2_adaptive_filter_destroy(&mut self.raw));
    }
}
