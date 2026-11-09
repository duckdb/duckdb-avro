//! `duckdb_v2_opaque`: how Rust data travels through the engine's `user_data` / `bind_data` /
//! `init_data` / `global_state` / `local_state` slots. The engine stores the pointer and calls the
//! destroy callback when it is done with the slot; the equality callback is what makes two bound call
//! sites with equal bind data compare equal.

use std::cell::Cell;
use std::ffi::c_void;
use std::marker::PhantomData;

use crate::sys::duckdb_v2_opaque;

/// Builds [`duckdb_v2_opaque`] handles around boxed Rust values.
pub struct Opaque;

unsafe extern "C" fn destroy_box<T>(ptr: *mut c_void) {
    if !ptr.is_null() {
        // SAFETY(ffi L8.I1): the pointer came from `Box::into_raw` in `Opaque::new` with this exact `T`.
        drop(unsafe { Box::from_raw(ptr as *mut T) });
    }
}

unsafe extern "C" fn equals_box<T: PartialEq>(a: *mut c_void, b: *mut c_void) -> bool {
    if a.is_null() || b.is_null() {
        return a == b;
    }
    // SAFETY(ffi L8.I1): both pointers came from `Opaque::with_equality` with this exact `T`.
    unsafe { *(a as *const T) == *(b as *const T) }
}

impl Opaque {
    /// Boxes `value`; the engine frees it through the destroy callback. Two slots compare by pointer.
    pub fn new<T: 'static>(value: T) -> duckdb_v2_opaque {
        duckdb_v2_opaque {
            ptr: Box::into_raw(Box::new(value)) as *mut c_void,
            destroy: Some(destroy_box::<T>),
            equals: None,
        }
    }

    /// Boxes `value`, comparing two slots by `PartialEq` (what bind data of a scalar function needs: the
    /// planner compares bound call sites).
    pub fn with_equality<T: PartialEq + 'static>(value: T) -> duckdb_v2_opaque {
        duckdb_v2_opaque {
            ptr: Box::into_raw(Box::new(value)) as *mut c_void,
            destroy: Some(destroy_box::<T>),
            equals: Some(equals_box::<T>),
        }
    }

    /// A borrowed, unowned pointer: the engine stores it and never frees it (for `'static` data).
    pub fn borrowed<T>(value: &'static T) -> duckdb_v2_opaque {
        duckdb_v2_opaque { ptr: value as *const T as *mut c_void, destroy: None, equals: None }
    }

    /// An empty slot.
    pub fn none() -> duckdb_v2_opaque {
        duckdb_v2_opaque { ptr: std::ptr::null_mut(), destroy: None, equals: None }
    }

    /// Reads back a slot that was filled by [`Opaque::new`] / [`Opaque::with_equality`] with the same `T`.
    /// `None` for an empty slot.
    ///
    /// # Safety
    /// (ffi L8.I1) `ptr` is null or the slot filled by [`Opaque::new`] / [`Opaque::with_equality`] with this `T`, kept
    /// alive by the engine for `'a`; no `&mut T` to it is live meanwhile.
    pub unsafe fn get<'a, T>(ptr: *mut c_void) -> Option<&'a T> {
        // SAFETY(ffi L8.I1): the caller asserts (this fn is `unsafe`) that the slot was filled by `Opaque::new` /
        // `with_equality` with this `T`.
        if ptr.is_null() { None } else { Some(unsafe { &*(ptr as *const T) }) }
    }

    /// The mutable form of [`Opaque::get`]. The engine does not synchronize slots that are shared between
    /// threads (a global state), so the caller is responsible for that.
    ///
    /// # Safety
    /// (ffi L8.I1) As [`Opaque::get`], and no other reference to the slot's value is live for `'a`.
    pub unsafe fn get_mut<'a, T>(ptr: *mut c_void) -> Option<&'a mut T> {
        // SAFETY(ffi L8.I1): as for `get`; the caller also guarantees no other reference to the slot is live.
        if ptr.is_null() { None } else { Some(unsafe { &mut *(ptr as *mut T) }) }
    }
}

/// The exclusive slots of a callback's info: the slots the C API hands to one callback at a
/// time - a thread's local state (`duckdb_v2.h`: "associated with the executing thread ... without synchronization"),
/// a scalar call's init data, a flush's batch data ("flushes of the same file never run concurrently"). An info that
/// carries this token hands each such slot out **once** per callback, as `&'cb mut T` where `'cb` is the trampoline's
/// frame (`CallbackSlots::scoped(&scope)` over a local of the trampoline), so the reference cannot outlive the callback
/// and no second reference to the same value exists while it lives; a second request of the slot in the same callback
/// is an error, not an alias. The shared slots of the same info (`bind_data`, `global_state`: "every thread reading the
/// file shares it, so the function must synchronize its own access to it") stay `&T`.
pub struct CallbackSlots<'cb> {
    taken: Cell<u8>,
    _callback: PhantomData<&'cb ()>,
}

impl<'cb> CallbackSlots<'cb> {
    /// The thread's local state (`*_get_local_state`).
    pub const LOCAL_STATE: u8 = 1;
    /// The scalar call's init data (`duckdb_v2_scalar_function_exec_get_init_data`).
    pub const INIT_DATA: u8 = 2;
    /// The batch data of a flush (`duckdb_v2_copy_to_flush_get_batch_data`).
    pub const BATCH_DATA: u8 = 4;
    /// The bind data of a call site in a bind-time callback the header lets write it (`duckdb_v2.h`, the propagate
    /// statistics callback: "to record that in the bind data"); the planner thread owns the call site then.
    pub const BIND_DATA: u8 = 8;

    /// The token of one callback invocation: `scope` is a local of the trampoline frame, so `'cb` ends with it.
    pub fn scoped(scope: &'cb ()) -> CallbackSlots<'cb> {
        let _ = scope;
        CallbackSlots { taken: Cell::new(0), _callback: PhantomData }
    }

    /// Records that `slot` is handed out for the rest of the callback; an error when it already was.
    pub fn take_exclusive(&self, slot: u8, what: &str) -> crate::Result<()> {
        let taken = self.taken.get();
        if taken & slot != 0 {
            return Err(crate::Error::internal(format!(
                "{what} was already taken by this callback: an exclusive slot is handed out once per callback"
            )));
        }
        self.taken.set(taken | slot);
        Ok(())
    }
}
