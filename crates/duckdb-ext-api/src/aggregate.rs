//! Aggregate functions: fixed-size states updated with argument vectors, combined and finalized.

use std::cell::Cell;
use std::ffi::c_void;
use std::marker::PhantomData;
use std::mem::{align_of, size_of};

use crate::Result;
use crate::context::{Context, Factory};
use crate::error::Error;
use crate::function::{ArgCounts, BindInfo, FunctionDocs, FunctionSignature, PropertyKey, PropertyValue};
use crate::opaque::Opaque;
use crate::statistics::Stats;
use crate::sys::*;
use crate::text::str_arg;
use crate::types::LogicalType;
use crate::vector::Vector;

/// Binds a call site (may set the return type and bind data).
pub trait AggregateBind {
    fn bind(bind: &BindInfo, info: &AggregateBindInfo, context: &Context) -> Result<()>;
}
/// Reports the byte size of one state.
pub trait AggregateSize {
    fn size(info: &AggregateSizeInfo) -> Result<()>;
}
/// Initializes states (zeroed memory of the reported size).
pub trait AggregateInit {
    fn init(info: &AggregateInitInfo) -> Result<()>;
}
/// Folds the argument vectors into the per-row states.
pub trait AggregateUpdate {
    fn update(info: &AggregateUpdateInfo) -> Result<()>;
}
/// Merges source states into target states.
pub trait AggregateCombine {
    fn combine(info: &AggregateCombineInfo) -> Result<()>;
}
/// Writes the result of each state into the result vector.
pub trait AggregateFinalize {
    fn finalize(info: &AggregateFinalizeInfo) -> Result<()>;
}
/// Releases what the states own (only needed for states with heap data).
pub trait AggregateDestroy {
    fn destroy(info: &AggregateDestroyInfo) -> Result<()>;
}

/// Called while the query is optimized, possibly more than once, to narrow the statistics of the result from those of
/// the arguments. Optional; without it the engine knows nothing about the result. Unstable API.
pub trait AggregateStats {
    fn stats(info: &AggregateStatsInfo, context: &Context) -> Result<()>;
}

trampoline!(
    stats_trampoline,
    AggregateStats::stats,
    AggregateStatsInfo,
    duckdb_v2_aggregate_function_stats_info_handle
);
bind_trampoline!(
    bind_trampoline,
    AggregateBind::bind,
    AggregateBindInfo,
    duckdb_v2_aggregate_function_bind_info_handle
);
info_trampoline!(
    size_trampoline,
    AggregateSize::size,
    AggregateSizeInfo,
    duckdb_v2_aggregate_function_size_info_handle
);
info_trampoline!(
    init_trampoline,
    AggregateInit::init,
    AggregateInitInfo,
    duckdb_v2_aggregate_function_init_info_handle
);
info_trampoline!(
    update_trampoline,
    AggregateUpdate::update,
    AggregateUpdateInfo,
    duckdb_v2_aggregate_function_update_info_handle
);
info_trampoline!(
    combine_trampoline,
    AggregateCombine::combine,
    AggregateCombineInfo,
    duckdb_v2_aggregate_function_combine_info_handle
);
info_trampoline!(
    finalize_trampoline,
    AggregateFinalize::finalize,
    AggregateFinalizeInfo,
    duckdb_v2_aggregate_function_finalize_info_handle
);
info_trampoline!(
    destroy_trampoline,
    AggregateDestroy::destroy,
    AggregateDestroyInfo,
    duckdb_v2_aggregate_function_destroy_info_handle
);

/// An aggregate function under construction (owned until registered).
pub struct AggregateFunction {
    raw: duckdb_v2_aggregate_function_handle,
    /// Set by [`AggregateFunction::set_state_callbacks`]: the state callbacks belong to one [`StateAggregate`] and the
    /// raw setters of those callbacks are refused from then on (the pairing of the size with the typed views).
    typed: Cell<bool>,
}

impl AggregateFunction {
    pub fn new(factory: Factory) -> Result<AggregateFunction> {
        let mut raw = std::ptr::null_mut();
        api_call!(duckdb_v2_aggregate_function_create(factory.as_raw(), &mut raw));
        Ok(AggregateFunction { raw, typed: Cell::new(false) })
    }

    /// The raw state callbacks (`set_size` .. `set_destroy`) after `set_state_callbacks`: refused.
    fn check_untyped(&self, setter: &str) -> Result<()> {
        if self.typed.get() {
            return Err(Error::invalid_input(format!(
                "AggregateFunction::{setter}: the state callbacks of this function were set by set_state_callbacks and \
                 cannot be replaced one by one"
            )));
        }
        Ok(())
    }

    pub fn as_raw(&self) -> duckdb_v2_aggregate_function_handle {
        self.raw
    }

    pub fn set_name(&self, name: &str) -> Result<&Self> {
        let name = str_arg(name);
        api_call!(duckdb_v2_aggregate_function_set_name(self.raw, &name));
        Ok(self)
    }

    pub fn signature(&self) -> Result<FunctionSignature> {
        Ok(api_call_out!(duckdb_v2_aggregate_function_get_signature(self.raw) -> FunctionSignature::from_raw))
    }

    pub fn docs(&self) -> Result<FunctionDocs> {
        Ok(api_call_out!(duckdb_v2_aggregate_function_get_docs(self.raw) -> FunctionDocs::from_raw))
    }

    pub fn set_user_data<T: 'static>(&self, data: T) -> Result<&Self> {
        let mut opaque = Opaque::new(data);
        api_call!(duckdb_v2_aggregate_function_set_user_data(self.raw, &mut opaque));
        Ok(self)
    }

    pub fn set_property(&self, key: PropertyKey, value: PropertyValue) -> Result<&Self> {
        api_call!(duckdb_v2_aggregate_function_set_property(self.raw, key, value));
        Ok(self)
    }

    pub fn set_bind<F: AggregateBind>(&self) -> Result<&Self> {
        api_call!(duckdb_v2_aggregate_function_set_bind_callback(self.raw, Some(bind_trampoline::<F>)));
        Ok(self)
    }
    pub fn set_size<F: AggregateSize>(&self) -> Result<&Self> {
        self.check_untyped("set_size")?;
        self.set_size_raw::<F>()
    }
    fn set_size_raw<F: AggregateSize>(&self) -> Result<&Self> {
        api_call!(duckdb_v2_aggregate_function_set_size_callback(self.raw, Some(size_trampoline::<F>)));
        Ok(self)
    }
    pub fn set_init<F: AggregateInit>(&self) -> Result<&Self> {
        self.check_untyped("set_init")?;
        self.set_init_raw::<F>()
    }
    fn set_init_raw<F: AggregateInit>(&self) -> Result<&Self> {
        api_call!(duckdb_v2_aggregate_function_set_init_callback(self.raw, Some(init_trampoline::<F>)));
        Ok(self)
    }
    pub fn set_update<F: AggregateUpdate>(&self) -> Result<&Self> {
        self.check_untyped("set_update")?;
        self.set_update_raw::<F>()
    }
    fn set_update_raw<F: AggregateUpdate>(&self) -> Result<&Self> {
        api_call!(duckdb_v2_aggregate_function_set_update_callback(self.raw, Some(update_trampoline::<F>)));
        Ok(self)
    }
    pub fn set_combine<F: AggregateCombine>(&self) -> Result<&Self> {
        self.check_untyped("set_combine")?;
        self.set_combine_raw::<F>()
    }
    fn set_combine_raw<F: AggregateCombine>(&self) -> Result<&Self> {
        api_call!(duckdb_v2_aggregate_function_set_combine_callback(self.raw, Some(combine_trampoline::<F>)));
        Ok(self)
    }
    pub fn set_finalize<F: AggregateFinalize>(&self) -> Result<&Self> {
        self.check_untyped("set_finalize")?;
        self.set_finalize_raw::<F>()
    }
    fn set_finalize_raw<F: AggregateFinalize>(&self) -> Result<&Self> {
        api_call!(duckdb_v2_aggregate_function_set_finalize_callback(self.raw, Some(finalize_trampoline::<F>)));
        Ok(self)
    }
    /// The stats callback (unstable API): see [`AggregateStats`].
    pub fn set_stats<F: AggregateStats>(&self) -> Result<&Self> {
        api_call!(duckdb_v2_aggregate_function_set_stats_callback(self.raw, Some(stats_trampoline::<F>)));
        Ok(self)
    }

    pub fn set_destroy<F: AggregateDestroy>(&self) -> Result<&Self> {
        self.check_untyped("set_destroy")?;
        self.set_destroy_raw::<F>()
    }
    fn set_destroy_raw<F: AggregateDestroy>(&self) -> Result<&Self> {
        api_call!(duckdb_v2_aggregate_function_set_destroy_callback(self.raw, Some(destroy_trampoline::<F>)));
        Ok(self)
    }

    /// Registers the state callbacks of a [`StateAggregate`] together: size (`size_of::<T::State>()`), init, update,
    /// combine, finalize and - for a `State` with drop glue - destroy, all for the one state type. Fails for a `State`
    /// aligned above 8 bytes (the engine's state alignment). From then on `set_size` .. `set_destroy` are refused: the
    /// typed callbacks view the states as `T::State` because the size they were registered with is that type's.
    pub fn set_state_callbacks<T: StateAggregate>(&self) -> Result<&Self> {
        let align = align_of::<T::State>();
        if align > STATE_ALIGNMENT {
            return Err(Error::invalid_input(format!(
                "AggregateFunction::set_state_callbacks: the state type is aligned to {align} bytes, above the \
                 {STATE_ALIGNMENT}-byte alignment of the engine's aggregate states"
            )));
        }
        self.set_size_raw::<StateCallbacks<T>>()?
            .set_init_raw::<StateCallbacks<T>>()?
            .set_update_raw::<StateCallbacks<T>>()?
            .set_combine_raw::<StateCallbacks<T>>()?
            .set_finalize_raw::<StateCallbacks<T>>()?;
        if std::mem::needs_drop::<T::State>() {
            self.set_destroy_raw::<StateCallbacks<T>>()?;
        }
        self.typed.set(true);
        Ok(self)
    }
}

/// The alignment the engine gives every aggregate state (the row layout's; L4.I1).
const STATE_ALIGNMENT: usize = 8;

/// The typed form of the aggregate callbacks (the C++ `AggregateFunction::UnaryAggregate<STATE, ...>` templates): one
/// state type for size, init, update, combine, finalize and destroy, registered together by
/// [`AggregateFunction::set_state_callbacks`], so that the pairing of the engine's state pointers with their type is
/// argued once, in this crate, and the callbacks see a [`States`] view and stay safe code.
/// The callback-shaped traits ([`AggregateSize`] .. [`AggregateDestroy`]) remain for a function whose states are not
/// one Rust type.
pub trait StateAggregate {
    /// The state: `size_of::<State>()` is what the size callback reports, so the type must be aligned to at most 8
    /// bytes (the engine's state alignment; checked at registration).
    type State;

    /// One fresh state (the init callback writes it into every state of the call).
    fn init() -> Self::State;

    /// Folds the argument vectors (`info.arg(i)`, `info.row_count()` rows) into the per-row states: input row `i`
    /// belongs to `states.get_mut(i)` (rows of one group share a state).
    fn update(info: &AggregateUpdateInfo, states: &mut States<'_, Self::State>) -> Result<()>;

    /// Merges `sources.get(i)` into `targets.get_mut(i)`.
    fn combine(
        info: &AggregateCombineInfo,
        sources: &States<'_, Self::State>,
        targets: &mut States<'_, Self::State>,
    ) -> Result<()>;

    /// Writes state `i` to row `info.result_offset() + i` of `info.result()`.
    fn finalize(info: &AggregateFinalizeInfo, states: &States<'_, Self::State>) -> Result<()>;

    /// Releases what the states own (registered only when `State` has drop glue); the default drops every state in
    /// place.
    fn destroy(info: &AggregateDestroyInfo, states: &mut States<'_, Self::State>) -> Result<()> {
        let _ = info;
        for index in 0..states.len() {
            states.drop_in_place(index);
        }
        Ok(())
    }
}

/// The callback types [`AggregateFunction::set_state_callbacks`] registers for a [`StateAggregate`].
struct StateCallbacks<T>(PhantomData<T>);

/// The states of a callback of a [`StateAggregate`] registration as `T::State`s: the one place the typed form turns
/// the engine's state pointers into a view.
fn typed_states<'a, T: StateAggregate>(ptrs: &'a [*mut c_void]) -> States<'a, T::State> {
    // SAFETY(layout L4.I1): `set_state_callbacks::<T>` registered the size callback (`size_of::<T::State>()`, the
    // alignment checked against the engine's there) together with the callback this view is formed in, and refuses the
    // raw setters afterwards, so every pointer the engine hands this callback addresses a `T::State`-sized, aligned
    // slot of this function, lent exclusively for the callback (`States::new`'s contract).
    unsafe { States::new(ptrs) }
}

impl<T: StateAggregate> AggregateSize for StateCallbacks<T> {
    fn size(info: &AggregateSizeInfo) -> Result<()> {
        info.set_state_size(size_of::<T::State>())
    }
}

impl<T: StateAggregate> AggregateInit for StateCallbacks<T> {
    fn init(info: &AggregateInitInfo) -> Result<()> {
        let mut states = typed_states::<T>(info.states()?);
        for index in 0..states.len() {
            states.write(index, T::init());
        }
        Ok(())
    }
}

impl<T: StateAggregate> AggregateUpdate for StateCallbacks<T> {
    fn update(info: &AggregateUpdateInfo) -> Result<()> {
        T::update(info, &mut typed_states::<T>(info.states()?))
    }
}

impl<T: StateAggregate> AggregateCombine for StateCallbacks<T> {
    fn combine(info: &AggregateCombineInfo) -> Result<()> {
        let sources = typed_states::<T>(info.sources()?);
        T::combine(info, &sources, &mut typed_states::<T>(info.targets()?))
    }
}

impl<T: StateAggregate> AggregateFinalize for StateCallbacks<T> {
    fn finalize(info: &AggregateFinalizeInfo) -> Result<()> {
        T::finalize(info, &typed_states::<T>(info.states()?))
    }
}

impl<T: StateAggregate> AggregateDestroy for StateCallbacks<T> {
    fn destroy(info: &AggregateDestroyInfo) -> Result<()> {
        T::destroy(info, &mut typed_states::<T>(info.states()?))
    }
}

impl Drop for AggregateFunction {
    fn drop(&mut self) {
        api_destroy!(duckdb_v2_aggregate_function_destroy(&mut self.raw));
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

macro_rules! states_getter {
    ($(#[$doc:meta])* $name:ident, $api:ident, $count_api:ident) => {
        $(#[$doc])*
        pub fn $name(&self) -> Result<&[*mut c_void]> {
            let mut count: idx_t = 0;
            api_call!($count_api(self.raw, &mut count));
            let mut states: *mut *mut c_void = std::ptr::null_mut();
            api_call!($api(self.raw, &mut states));
            if count == 0 || states.is_null() {
                return Ok(&[]);
            }
            // SAFETY(ffi L8.I1): the engine hands out `count` state pointers, live for the callback.
            Ok(unsafe { std::slice::from_raw_parts(states, count as usize) })
        }
    };
}

/// The states of one aggregate callback viewed as `T` (the `STATE` of the C++ `AggregateFunction` templates): one
/// place turns the engine's `void *` state pointers into references, so the callback bodies stay safe code.
pub struct States<'a, T> {
    ptrs: &'a [*mut c_void],
    _state: std::marker::PhantomData<&'a mut T>,
}

impl<'a, T> States<'a, T> {
    /// Views `ptrs` (`states()` / `sources()` / `targets()` of an aggregate info) as states of type `T`.
    ///
    /// # Safety
    ///
    /// (layout L4.I1) `T` is the state type of this aggregate: the size callback declared `size_of::<T>()`
    /// (`set_state_size`), so every pointer addresses `size_of::<T>()` writable bytes aligned for `T` that live for the
    /// callback, and no other reference to those states is live while this view is.
    pub unsafe fn new(ptrs: &'a [*mut c_void]) -> States<'a, T> {
        States { ptrs, _state: std::marker::PhantomData }
    }

    pub fn len(&self) -> usize {
        self.ptrs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ptrs.is_empty()
    }

    /// Initializes state `index` (the init callback: the memory holds no `T` yet, so this writes without dropping).
    pub fn write(&mut self, index: usize, state: T) {
        let ptr = self.ptrs[index] as *mut T;
        // SAFETY(layout L4.I1): `new`'s contract - `ptr` addresses `size_of::<T>()` writable bytes aligned for `T`.
        unsafe { ptr.write(state) };
    }

    /// State `index` (an initialized `T`).
    pub fn get(&self, index: usize) -> &T {
        let ptr = self.ptrs[index] as *const T;
        // SAFETY(layout L4.I1): `new`'s contract, and no `&mut T` is live while `&self` is borrowed.
        unsafe { &*ptr }
    }

    /// State `index` for writing. Rows of an update share pointers (one state per group), so one state is borrowed at
    /// a time (`&mut self`).
    pub fn get_mut(&mut self, index: usize) -> &mut T {
        let ptr = self.ptrs[index] as *mut T;
        // SAFETY(layout L4.I1): `new`'s contract, and `&mut self` keeps this the only reference into the states.
        unsafe { &mut *ptr }
    }

    /// Drops state `index` in place (the destroy callback: the engine never reads the state again).
    pub(crate) fn drop_in_place(&mut self, index: usize) {
        let ptr = self.ptrs[index] as *mut T;
        // SAFETY(layout L4.I1): `new`'s contract (an initialized `T` the engine lends exclusively for the destroy
        // callback), and `&mut self` keeps this the only reference into the states; the slot is not read afterwards.
        unsafe { ptr.drop_in_place() };
    }
}

/// The stats callback's info (borrowed, valid only for the callback duration).
///
/// The result statistics start out unknown, with every kind of value possible. They describe every result the
/// aggregate produces, including that of a group with no input rows, such as an ungrouped aggregate over an empty
/// table. A callback that leaves them unchanged tells the engine nothing.
#[derive(Clone, Copy)]
pub struct AggregateStatsInfo {
    raw: duckdb_v2_aggregate_function_stats_info_handle,
}

impl AggregateStatsInfo {
    data_getter!(/// The function's user data.
        user_data, duckdb_v2_aggregate_function_stats_get_user_data);
    data_getter!(/// The bind data of the call.
        bind_data, duckdb_v2_aggregate_function_stats_get_bind_data);

    /// The sizes of the four parts of the argument list, as the bind callback saw them.
    pub fn arg_counts(&self) -> Result<ArgCounts> {
        let (mut a, mut b, mut c, mut d): (idx_t, idx_t, idx_t, idx_t) = (0, 0, 0, 0);
        api_call!(duckdb_v2_aggregate_function_stats_get_arg_count(self.raw, &mut a, &mut b, &mut c, &mut d));
        Ok(ArgCounts {
            positional_fixed: a as usize,
            positional_variadic: b as usize,
            named_fixed: c as usize,
            named_variadic: d as usize,
        })
    }

    /// How many arguments the call has, in all four parts: the valid indices of [`AggregateStatsInfo::arg_stats`].
    pub fn arg_count(&self) -> Result<usize> {
        Ok(self.arg_counts()?.total())
    }

    /// The statistics of one argument, at the index the bind callback used for it. Read-only.
    pub fn arg_stats(&self, index: usize) -> Result<Stats> {
        let mut raw: duckdb_v2_stats_handle = std::ptr::null_mut();
        api_call!(duckdb_v2_aggregate_function_stats_get_arg_stats(self.raw, index as idx_t, &mut raw));
        // SAFETY(ffi L8.I1): the engine lends a live stats handle for the duration of the callback.
        Ok(unsafe { Stats::from_raw(raw) })
    }

    /// The statistics of the result, for the callback to narrow.
    pub fn result_stats(&self) -> Result<Stats> {
        let mut raw: duckdb_v2_stats_handle = std::ptr::null_mut();
        api_call!(duckdb_v2_aggregate_function_stats_get_result_stats(self.raw, &mut raw));
        // SAFETY(ffi L8.I1): the engine lends a live stats handle for the duration of the callback.
        Ok(unsafe { Stats::from_raw(raw) })
    }
}

/// The bind callback's info.
#[derive(Clone, Copy)]
pub struct AggregateBindInfo {
    raw: duckdb_v2_aggregate_function_bind_info_handle,
}

impl AggregateBindInfo {
    /// The return type of this call (when the signature leaves it open).
    pub fn set_return_type(&self, type_: &LogicalType) -> Result<()> {
        api_call!(duckdb_v2_aggregate_function_bind_set_return_type(self.raw, type_.as_raw()));
        Ok(())
    }
}

/// The size callback's info.
#[derive(Clone, Copy)]
pub struct AggregateSizeInfo {
    raw: duckdb_v2_aggregate_function_size_info_handle,
}

impl AggregateSizeInfo {
    data_getter!(/// The function's user data.
        user_data, duckdb_v2_aggregate_function_size_get_user_data);
    data_getter!(/// The bind data of the call.
        bind_data, duckdb_v2_aggregate_function_size_get_bind_data);

    pub fn set_state_size(&self, size: usize) -> Result<()> {
        api_call!(duckdb_v2_aggregate_function_size_set_state_size(self.raw, size as idx_t));
        Ok(())
    }
}

/// The init callback's info.
#[derive(Clone, Copy)]
pub struct AggregateInitInfo {
    raw: duckdb_v2_aggregate_function_init_info_handle,
}

impl AggregateInitInfo {
    data_getter!(/// The function's user data.
        user_data, duckdb_v2_aggregate_function_init_get_user_data);
    data_getter!(/// The bind data of the call.
        bind_data, duckdb_v2_aggregate_function_init_get_bind_data);
    states_getter!(/// The states to initialize.
        states, duckdb_v2_aggregate_function_init_get_states, duckdb_v2_aggregate_function_init_get_state_count);
}

/// The update callback's info.
#[derive(Clone, Copy)]
pub struct AggregateUpdateInfo {
    raw: duckdb_v2_aggregate_function_update_info_handle,
}

impl AggregateUpdateInfo {
    data_getter!(/// The function's user data.
        user_data, duckdb_v2_aggregate_function_update_get_user_data);
    data_getter!(/// The bind data of the call.
        bind_data, duckdb_v2_aggregate_function_update_get_bind_data);

    pub fn row_count(&self) -> Result<usize> {
        let mut count: idx_t = 0;
        api_call!(duckdb_v2_aggregate_function_update_get_row_count(self.raw, &mut count));
        Ok(count as usize)
    }

    pub fn arg_counts(&self) -> Result<ArgCounts> {
        let (mut a, mut b, mut c, mut d): (idx_t, idx_t, idx_t, idx_t) = (0, 0, 0, 0);
        api_call!(duckdb_v2_aggregate_function_update_get_arg_count(self.raw, &mut a, &mut b, &mut c, &mut d));
        Ok(ArgCounts {
            positional_fixed: a as usize,
            positional_variadic: b as usize,
            named_fixed: c as usize,
            named_variadic: d as usize,
        })
    }

    pub fn arg(&self, index: usize) -> Result<Vector> {
        Ok(api_call_out!(duckdb_v2_aggregate_function_update_get_arg(self.raw, index as idx_t) -> Vector::from_raw))
    }

    /// One state pointer per row (rows of the same group share a pointer).
    pub fn states(&self) -> Result<&[*mut c_void]> {
        let count = self.row_count()?;
        let mut states: *mut *mut c_void = std::ptr::null_mut();
        api_call!(duckdb_v2_aggregate_function_update_get_states(self.raw, &mut states));
        if count == 0 || states.is_null() {
            return Ok(&[]);
        }
        // SAFETY(ffi L8.I1): the engine hands out one state pointer per row, live for the callback.
        Ok(unsafe { std::slice::from_raw_parts(states, count) })
    }
}

/// The combine callback's info.
#[derive(Clone, Copy)]
pub struct AggregateCombineInfo {
    raw: duckdb_v2_aggregate_function_combine_info_handle,
}

impl AggregateCombineInfo {
    data_getter!(/// The function's user data.
        user_data, duckdb_v2_aggregate_function_combine_get_user_data);
    data_getter!(/// The bind data of the call.
        bind_data, duckdb_v2_aggregate_function_combine_get_bind_data);
    states_getter!(/// The states to merge from.
        sources, duckdb_v2_aggregate_function_combine_get_sources, duckdb_v2_aggregate_function_combine_get_state_count);
    states_getter!(/// The states to merge into (parallel to the sources).
        targets, duckdb_v2_aggregate_function_combine_get_targets, duckdb_v2_aggregate_function_combine_get_state_count);
}

/// The finalize callback's info.
#[derive(Clone, Copy)]
pub struct AggregateFinalizeInfo {
    raw: duckdb_v2_aggregate_function_finalize_info_handle,
}

impl AggregateFinalizeInfo {
    data_getter!(/// The function's user data.
        user_data, duckdb_v2_aggregate_function_finalize_get_user_data);
    data_getter!(/// The bind data of the call.
        bind_data, duckdb_v2_aggregate_function_finalize_get_bind_data);
    states_getter!(/// The states to finalize.
        states, duckdb_v2_aggregate_function_finalize_get_states, duckdb_v2_aggregate_function_finalize_get_state_count);

    /// The result vector; state `i` writes row `result_offset() + i`.
    pub fn result(&self) -> Result<Vector> {
        Ok(api_call_out!(duckdb_v2_aggregate_function_finalize_get_result(self.raw) -> Vector::from_raw))
    }

    pub fn result_offset(&self) -> Result<usize> {
        let mut offset: idx_t = 0;
        api_call!(duckdb_v2_aggregate_function_finalize_get_result_offset(self.raw, &mut offset));
        Ok(offset as usize)
    }
}

/// The destroy callback's info.
#[derive(Clone, Copy)]
pub struct AggregateDestroyInfo {
    raw: duckdb_v2_aggregate_function_destroy_info_handle,
}

impl AggregateDestroyInfo {
    data_getter!(/// The function's user data.
        user_data, duckdb_v2_aggregate_function_destroy_get_user_data);
    data_getter!(/// The bind data of the call.
        bind_data, duckdb_v2_aggregate_function_destroy_get_bind_data);
    states_getter!(/// The states to release.
        states, duckdb_v2_aggregate_function_destroy_get_states, duckdb_v2_aggregate_function_destroy_get_state_count);
}
