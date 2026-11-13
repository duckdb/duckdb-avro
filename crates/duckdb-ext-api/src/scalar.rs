//! Scalar functions.

use crate::Result;
use crate::context::{Context, Factory};
use crate::function::{ArgCounts, BindInfo, FunctionDocs, FunctionSignature, PropertyKey, PropertyValue};
use crate::opaque::{CallbackSlots, Opaque};
use crate::statistics::Stats;
use crate::sys::*;
use crate::text::str_arg;
use crate::types::LogicalType;
use crate::vector::Vector;

/// `DUCKDB_V2_MONOTONICITY`: how the result moves with one argument (`ArgProperties`).
pub type Monotonicity = DUCKDB_V2_MONOTONICITY;

/// Resolves the return type of a call whose signature leaves it open (an `ANY` return type).
pub trait ScalarResolveTypes {
    fn resolve_types(bind: &BindInfo, info: &ScalarResolveTypesInfo, context: &Context) -> Result<()>;
}

/// Binds a call site: inspects the arguments and stores bind data.
pub trait ScalarBind {
    fn bind(bind: &BindInfo, info: &ScalarBindInfo, context: &Context) -> Result<()>;
}

/// Initializes per-thread state before execution.
pub trait ScalarInit {
    fn init(info: &ScalarInitInfo, context: &Context) -> Result<()>;
}

/// Computes the result vector from the argument vectors.
pub trait ScalarExec {
    fn exec(info: &ScalarExecInfo<'_>, context: &Context) -> Result<()>;
}

/// Called while the query is optimized, possibly more than once, to narrow the statistics of the result from those of
/// the arguments. Optional; without it the engine knows nothing about the result. Unstable API.
pub trait ScalarStats {
    fn stats(info: &ScalarStatsInfo, context: &Context) -> Result<()>;
}

bind_trampoline!(
    resolve_types_trampoline,
    ScalarResolveTypes::resolve_types,
    ScalarResolveTypesInfo,
    duckdb_v2_scalar_function_resolve_types_info_handle
);
bind_trampoline!(bind_trampoline, ScalarBind::bind, ScalarBindInfo, duckdb_v2_scalar_function_bind_info_handle);
trampoline!(init_trampoline, ScalarInit::init, ScalarInitInfo, duckdb_v2_scalar_function_init_info_handle);
trampoline_cb!(exec_trampoline, ScalarExec::exec, ScalarExecInfo, duckdb_v2_scalar_function_exec_info_handle);
trampoline!(stats_trampoline, ScalarStats::stats, ScalarStatsInfo, duckdb_v2_scalar_function_stats_info_handle);

/// A scalar function under construction (owned until registered).
pub struct ScalarFunction {
    raw: duckdb_v2_scalar_function_handle,
}

impl ScalarFunction {
    pub fn new(factory: Factory) -> Result<ScalarFunction> {
        let mut raw = std::ptr::null_mut();
        api_call!(duckdb_v2_scalar_function_create(factory.as_raw(), &mut raw));
        Ok(ScalarFunction { raw })
    }

    pub fn as_raw(&self) -> duckdb_v2_scalar_function_handle {
        self.raw
    }

    pub fn set_name(&self, name: &str) -> Result<&Self> {
        let name = str_arg(name);
        api_call!(duckdb_v2_scalar_function_set_name(self.raw, &name));
        Ok(self)
    }

    pub fn signature(&self) -> Result<FunctionSignature> {
        Ok(api_call_out!(duckdb_v2_scalar_function_get_signature(self.raw) -> FunctionSignature::from_raw))
    }

    pub fn docs(&self) -> Result<FunctionDocs> {
        Ok(api_call_out!(duckdb_v2_scalar_function_get_docs(self.raw) -> FunctionDocs::from_raw))
    }

    /// Data every callback can read back (`user_data::<T>()`).
    pub fn set_user_data<T: 'static>(&self, data: T) -> Result<&Self> {
        let mut opaque = Opaque::new(data);
        api_call!(duckdb_v2_scalar_function_set_user_data(self.raw, &mut opaque));
        Ok(self)
    }

    /// Stability, NULL handling, fallibility, collation handling.
    pub fn set_property(&self, key: PropertyKey, value: PropertyValue) -> Result<&Self> {
        api_call!(duckdb_v2_scalar_function_set_property(self.raw, key, value));
        Ok(self)
    }

    /// `SetArgProperties(index, ArgProperties().NonDecreasing() / ...)`: how the result moves with the argument at
    /// `index` (the optimizer rewrites comparisons of the result into comparisons of the argument). newer engines only:
    /// a no-op for a library without the entry.
    pub fn set_arg_monotonicity(&self, index: usize, monotonicity: Monotonicity) -> Result<&Self> {
        if crate::api::api().duckdb_v2_scalar_function_set_arg_monotonicity.is_none() {
            return Ok(self);
        }
        api_call!(duckdb_v2_scalar_function_set_arg_monotonicity(self.raw, index as idx_t, monotonicity));
        Ok(self)
    }

    /// `SetStatisticsCallback(StatisticsPropagator::PropagateFiniteMonotoneBounds)`: the engine derives the bounds of
    /// the result from the declared monotonicity, except when the range of an argument holds -infinity, for which the
    /// function returns NULL. newer engines only: a no-op for a library without the entry.
    pub fn set_finite_monotone_stats(&self) -> Result<&Self> {
        if crate::api::api().duckdb_v2_scalar_function_set_finite_monotone_stats.is_none() {
            return Ok(self);
        }
        api_call!(duckdb_v2_scalar_function_set_finite_monotone_stats(self.raw));
        Ok(self)
    }

    /// `FunctionProperties::SetRequiresExpressionNames()`: the result depends on the aliases of the argument
    /// expressions ([`crate::BindInfo::arg_alias`]), so the SQL export keeps the argument names of a bound call. Rust
    /// engine only: a no-op for a library without the entry.
    pub fn set_requires_expression_names(&self, requires_expression_names: bool) -> Result<&Self> {
        if crate::api::api().duckdb_v2_scalar_function_set_requires_expression_names.is_none() {
            return Ok(self);
        }
        api_call!(duckdb_v2_scalar_function_set_requires_expression_names(self.raw, requires_expression_names));
        Ok(self)
    }

    pub fn set_resolve_types<F: ScalarResolveTypes>(&self) -> Result<&Self> {
        api_call!(duckdb_v2_scalar_function_set_resolve_types_callback(self.raw, Some(resolve_types_trampoline::<F>)));
        Ok(self)
    }

    pub fn set_bind<F: ScalarBind>(&self) -> Result<&Self> {
        api_call!(duckdb_v2_scalar_function_set_bind_callback(self.raw, Some(bind_trampoline::<F>)));
        Ok(self)
    }

    pub fn set_init<F: ScalarInit>(&self) -> Result<&Self> {
        api_call!(duckdb_v2_scalar_function_set_init_callback(self.raw, Some(init_trampoline::<F>)));
        Ok(self)
    }

    pub fn set_exec<F: ScalarExec>(&self) -> Result<&Self> {
        api_call!(duckdb_v2_scalar_function_set_exec_callback(self.raw, Some(exec_trampoline::<F>)));
        Ok(self)
    }

    /// A raw `extern "C"` exec callback, for code that manages the error slot itself.
    pub fn set_exec_raw(&self, callback: duckdb_v2_scalar_function_exec_callback_fn) -> Result<&Self> {
        api_call!(duckdb_v2_scalar_function_set_exec_callback(self.raw, callback));
        Ok(self)
    }

    /// The stats callback (unstable API): see [`ScalarStats`].
    pub fn set_stats<F: ScalarStats>(&self) -> Result<&Self> {
        api_call!(duckdb_v2_scalar_function_set_stats_callback(self.raw, Some(stats_trampoline::<F>)));
        Ok(self)
    }
}

impl Drop for ScalarFunction {
    fn drop(&mut self) {
        api_destroy!(duckdb_v2_scalar_function_destroy(&mut self.raw));
    }
}

/// The resolve-types callback's info.
#[derive(Clone, Copy)]
pub struct ScalarResolveTypesInfo {
    raw: duckdb_v2_scalar_function_resolve_types_info_handle,
}

impl ScalarResolveTypesInfo {
    pub fn set_return_type(&self, type_: &LogicalType) -> Result<()> {
        api_call!(duckdb_v2_scalar_function_resolve_types_set_return_type(self.raw, type_.as_raw()));
        Ok(())
    }
}

/// The bind callback's info.
#[derive(Clone, Copy)]
pub struct ScalarBindInfo {
    raw: duckdb_v2_scalar_function_bind_info_handle,
}

impl ScalarBindInfo {
    /// Stores bind data; the planner compares bound call sites through `PartialEq`.
    pub fn set_bind_data<T: PartialEq + 'static>(&self, data: T) -> Result<()> {
        let mut opaque = Opaque::with_equality(data);
        api_call!(duckdb_v2_scalar_function_bind_set_bind_data(self.raw, &mut opaque));
        Ok(())
    }

    /// Binds the call as a call of `function_name` over the listed arguments of this call, in that order (the C++
    /// bind callbacks that rebind `bound_function` and `arguments`): the planner sees that function. `false` when
    /// the loading engine does not offer the entry, so that the caller binds the call itself.
    pub fn delegate(&self, function_name: &str, argument_indexes: &[usize]) -> Result<bool> {
        if crate::api::api().duckdb_v2_scalar_function_bind_delegate.is_none() {
            return Ok(false);
        }
        let name = str_arg(function_name);
        let indexes: Vec<idx_t> = argument_indexes.iter().map(|&index| index as idx_t).collect();
        api_call!(duckdb_v2_scalar_function_bind_delegate(self.raw, &name, indexes.as_ptr(), indexes.len() as idx_t));
        Ok(true)
    }

    /// Declares, for this call only, how the result moves with the argument at `index` (what a C++ bind callback
    /// does through `input.GetBoundFunction().SetArgProperties(...)`); UNKNOWN withdraws the declaration made when
    /// the function was registered. `false` when the loading engine does not offer the entry.
    pub fn set_arg_monotonicity(&self, index: usize, monotonicity: Monotonicity) -> Result<bool> {
        if crate::api::api().duckdb_v2_scalar_function_bind_set_arg_monotonicity.is_none() {
            return Ok(false);
        }
        api_call!(duckdb_v2_scalar_function_bind_set_arg_monotonicity(self.raw, index as idx_t, monotonicity));
        Ok(true)
    }
}

/// The init callback's info.
#[derive(Clone, Copy)]
pub struct ScalarInitInfo {
    raw: duckdb_v2_scalar_function_init_info_handle,
}

impl ScalarInitInfo {
    opaque_getter!(user_data, duckdb_v2_scalar_function_init_get_user_data);

    opaque_getter!(bind_data, duckdb_v2_scalar_function_init_get_bind_data);

    /// Stores the thread-local init data.
    pub fn set_init_data<T: 'static>(&self, data: T) -> Result<()> {
        let mut opaque = Opaque::new(data);
        api_call!(duckdb_v2_scalar_function_init_set_init_data(self.raw, &mut opaque));
        Ok(())
    }
}

/// The exec callback's info: the argument vectors and the result vector of one invocation.
pub struct ScalarExecInfo<'cb> {
    raw: duckdb_v2_scalar_function_exec_info_handle,
    /// The exclusive slots of this callback ([`CallbackSlots`]).
    slots: CallbackSlots<'cb>,
}

impl<'cb> ScalarExecInfo<'cb> {
    opaque_getter!(user_data, duckdb_v2_scalar_function_exec_get_user_data);

    opaque_getter!(bind_data, duckdb_v2_scalar_function_exec_get_bind_data);

    exclusive_getter!(
        /// The thread-local init data set by the init callback (`duckdb_v2.h`: "local state reusable across
        /// invocations" of the executing thread); handed out once per callback as `&mut` for the callback's duration.
        init_data, duckdb_v2_scalar_function_exec_get_init_data, INIT_DATA, "init_data");

    pub fn row_count(&self) -> Result<usize> {
        let mut count: idx_t = 0;
        api_call!(duckdb_v2_scalar_function_exec_get_row_count(self.raw, &mut count));
        Ok(count as usize)
    }

    pub fn arg_counts(&self) -> Result<ArgCounts> {
        let (mut a, mut b, mut c, mut d): (idx_t, idx_t, idx_t, idx_t) = (0, 0, 0, 0);
        api_call!(duckdb_v2_scalar_function_exec_get_arg_count(self.raw, &mut a, &mut b, &mut c, &mut d));
        Ok(ArgCounts {
            positional_fixed: a as usize,
            positional_variadic: b as usize,
            named_fixed: c as usize,
            named_variadic: d as usize,
        })
    }

    pub fn arg(&self, index: usize) -> Result<Vector> {
        Ok(api_call_out!(duckdb_v2_scalar_function_exec_get_arg(self.raw, index as idx_t) -> Vector::from_raw))
    }

    /// The FLAT result vector, sized to the row count.
    pub fn result(&self) -> Result<Vector> {
        Ok(api_call_out!(duckdb_v2_scalar_function_exec_get_result(self.raw) -> Vector::from_raw))
    }
}

/// The stats callback's info (borrowed, valid only for the callback duration).
///
/// The result statistics start out unknown, with every kind of value possible. A callback that leaves them
/// unchanged tells the engine nothing.
#[derive(Clone, Copy)]
pub struct ScalarStatsInfo {
    raw: duckdb_v2_scalar_function_stats_info_handle,
}

impl ScalarStatsInfo {
    /// The user data set via [`ScalarFunction::set_user_data`].
    pub fn user_data<T>(&self) -> Result<Option<&T>> {
        let mut data: *mut c_void = std::ptr::null_mut();
        api_call!(duckdb_v2_scalar_function_stats_get_user_data(self.raw, &mut data));
        // SAFETY(ffi L8.I1): the slot holds what `set_user_data` stored for the `T` the caller names, or is empty.
        Ok(unsafe { Opaque::get::<T>(data) })
    }

    /// The bind data set via `BindInfo::set_bind_data`.
    pub fn bind_data<T>(&self) -> Result<Option<&T>> {
        let mut data: *mut c_void = std::ptr::null_mut();
        api_call!(duckdb_v2_scalar_function_stats_get_bind_data(self.raw, &mut data));
        // SAFETY(ffi L8.I1): the slot holds what `set_bind_data` stored for the `T` the caller names, or is empty.
        Ok(unsafe { Opaque::get::<T>(data) })
    }

    /// The sizes of the four parts of the argument list, as the bind callback saw them.
    pub fn arg_counts(&self) -> Result<ArgCounts> {
        let (mut a, mut b, mut c, mut d): (idx_t, idx_t, idx_t, idx_t) = (0, 0, 0, 0);
        api_call!(duckdb_v2_scalar_function_stats_get_arg_count(self.raw, &mut a, &mut b, &mut c, &mut d));
        Ok(ArgCounts {
            positional_fixed: a as usize,
            positional_variadic: b as usize,
            named_fixed: c as usize,
            named_variadic: d as usize,
        })
    }

    /// How many arguments the call has, in all four parts: the valid indices of [`ScalarStatsInfo::arg_stats`].
    pub fn arg_count(&self) -> Result<usize> {
        Ok(self.arg_counts()?.total())
    }

    /// The statistics of one argument, at the index the bind callback used for it. Read-only.
    pub fn arg_stats(&self, index: usize) -> Result<Stats> {
        let mut raw: duckdb_v2_stats_handle = std::ptr::null_mut();
        api_call!(duckdb_v2_scalar_function_stats_get_arg_stats(self.raw, index as idx_t, &mut raw));
        // SAFETY(ffi L8.I1): the engine lends a live stats handle for the duration of the callback.
        Ok(unsafe { Stats::from_raw(raw) })
    }

    /// The statistics of the result, for the callback to narrow.
    pub fn result_stats(&self) -> Result<Stats> {
        let mut raw: duckdb_v2_stats_handle = std::ptr::null_mut();
        api_call!(duckdb_v2_scalar_function_stats_get_result_stats(self.raw, &mut raw));
        // SAFETY(ffi L8.I1): as for `arg_stats`.
        Ok(unsafe { Stats::from_raw(raw) })
    }
}
