//! The `duckdb_ext_api_v2` function table: fetched once from the engine through `get_api` and kept
//! process-wide, as the C `DUCKDB_EXTENSION_GLOBAL` / `DUCKDB_EXTENSION_API_INIT` pair does.

use std::ffi::{CString, c_void};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::sys;
use crate::sys::{
    API_TABLE_FUNCTION_NAMES, DUCKDB_EXTENSION_API_VERSION_STRING, UPSTREAM_API_TABLE_SIZE, duckdb_ext_api_v2,
    duckdb_v2_extension_input,
};

/// The table is plain function pointers (`Option<unsafe extern "C" fn>` fields, `Send + Sync` by the auto traits);
/// sharing it between threads is what the engine expects.
struct ApiTable(duckdb_ext_api_v2);

static API: OnceLock<ApiTable> = OnceLock::new();

/// The byte size of the function table the loading engine hands out, as a newer engine's loader announced it
/// through [`duckdb_capi_announce_api_table_size`] before calling the entrypoint; 0 until an engine announces one (an
/// upstream engine never does: its table is the upstream prefix, see [`copy_table`]).
static ANNOUNCED_TABLE_SIZE: AtomicUsize = AtomicUsize::new(0);

/// A newer engine's loader announces the size of its `duckdb_ext_api_v2` table: before it calls the
/// `<name>_init_c_api_v2` entrypoint of a `dlopen`ed extension it looks this symbol up in the extension's library
/// and, when the extension
/// exports it - every binary of this crate does - calls it with `duckdb_v2_library_api_table_size()`'s value; for
/// a statically linked extension, which shares the engine binary's copy of this crate, the engine calls the
/// function directly. The entrypoint then knows how much of the table
/// `get_api` hands out it may read (`copy_table`). A stock C++ engine does not know the symbol and never calls
/// it, so its extensions see the upstream prefix only; nothing about the engine's version string is involved.
///
/// The announcement is per process (an extension is loaded by one engine, every load of one library in a process
/// receives the same table) and idempotent.
#[unsafe(no_mangle)]
pub extern "C" fn duckdb_capi_announce_api_table_size(size: sys::idx_t) {
    ANNOUNCED_TABLE_SIZE.store(usize::try_from(size).unwrap_or(usize::MAX), Ordering::Release);
}

/// The table size the loading engine announced ([`duckdb_capi_announce_api_table_size`]); 0 when none did.
pub fn announced_api_table_size() -> usize {
    ANNOUNCED_TABLE_SIZE.load(Ordering::Acquire)
}

/// The C API version this crate is built against (`DUCKDB_EXTENSION_API_VERSION_STRING`): what
/// `get_api` is asked for.
pub fn api_version_string() -> &'static str {
    DUCKDB_EXTENSION_API_VERSION_STRING
}

/// `DUCKDB_EXTENSION_API_INIT`: asks the engine for the function table of [`api_version_string`] and
/// keeps a copy. Returns `false` when the engine refuses (the version is unsupported); the load has then
/// already failed on the engine side and the entrypoint must return without touching the error slot.
///
/// Every entrypoint run asks (the engine treats an entrypoint that never called `get_api` as broken and fails
/// the load, and the call is its version handshake). A second initialization (the same binary loaded into
/// several databases of one process) keeps the first copy of the table: the engine builds identical tables for
/// one library.
pub fn init_api(input: &duckdb_v2_extension_input) -> bool {
    init_api_with_version(input, api_version_string())
}

/// [`init_api`] asking `get_api` for `version` instead of [`api_version_string`]: what an extension that declares
/// the C API version it needs (a version-pinned entrypoint) passes. A
/// version with an embedded NUL cannot be asked for and is refused.
pub fn init_api_with_version(input: &duckdb_v2_extension_input, version: &str) -> bool {
    let Some(get_api) = input.get_api else {
        // the engine lent no table (a static load by a loader that does not hand it out): only a table fetched
        // by an earlier load can serve
        return API.get().is_some();
    };
    let Ok(version) = CString::new(version) else {
        return false;
    };
    // SAFETY(ffi): `get_api` is the engine's callback of the entrypoint input, `extension` the handle it lent for this
    // load, and `version` a NUL-terminated string that outlives the call.
    let table = unsafe { get_api(input.extension, version.as_ptr()) } as *const duckdb_ext_api_v2;
    if table.is_null() {
        return false;
    }
    if API.get().is_none() {
        // SAFETY(ffi): the engine hands out a live `duckdb_ext_api_v2` of at least the upstream layout, and of the
        // announced size when its loader announced one.
        let copy = unsafe { copy_table(table, announced_api_table_size()) };
        let _ = API.set(ApiTable(copy));
    }
    true
}

/// Copies the table an engine handed out - as much of it as the engine provides.
///
/// Every engine implementing C API v2.0.0 provides the upstream table (`UPSTREAM_API_TABLE_SIZE` bytes: the stable
/// surface plus the unstable entries of the upstream header). Newer engines append entries after it
/// (its C API additions). Reading the whole Rust-side struct from an upstream engine's table would
/// read past its end, so only the upstream prefix is copied unless the engine said how big its table is: the Rust
/// engine's loader announces the size through [`duckdb_capi_announce_api_table_size`] before it runs the entrypoint
/// (`announced`, the value `duckdb_v2_library_api_table_size()` reports). That many bytes are copied and the
/// entries beyond stay NULL, so an extension newer than its engine sees the missing functions as absent
/// (`Error::missing_entry`) instead of calling into garbage; an announcement shorter than the prefix (no
/// announcement, or a nonsensical one) copies the prefix. Nothing is called through the table here.
///
/// # Safety
/// (ffi) `table` points at the live `duckdb_ext_api_v2` the engine handed to the entrypoint (or `dlsym` filled),
/// readable for at least the upstream prefix, and for `announced` bytes when `announced` exceeds it.
unsafe fn copy_table(table: *const duckdb_ext_api_v2, announced: usize) -> duckdb_ext_api_v2 {
    let total = std::mem::size_of::<duckdb_ext_api_v2>();
    let mut copy = std::mem::MaybeUninit::<duckdb_ext_api_v2>::zeroed();
    let dst = copy.as_mut_ptr() as *mut u8;
    let src = table as *const u8;
    // the Rust-side struct is the upstream prefix plus the appended band: never read more than it holds
    let copied = UPSTREAM_API_TABLE_SIZE.max(announced).min(total);
    // SAFETY(ffi): the engine's table has at least the upstream layout and, when its loader announced a size, that
    // many readable bytes; a zeroed table is all-`None` function pointers.
    unsafe { std::ptr::copy_nonoverlapping(src, dst, copied) };
    // SAFETY(ffi): every byte is either copied from the engine's table or zero (`None`); the table type has no invalid
    // bit patterns besides non-null garbage, and the engine's entries are function pointers or NULL.
    unsafe { copy.assume_init() }
}

/// Whether [`init_api`] or [`install_table`] succeeded in this process.
pub fn is_api_initialized() -> bool {
    API.get().is_some()
}

/// Installs a table a *client* of the C API built itself - from the symbols of a `dlopen`ed library
/// ([`table_from_symbols`], [`crate::client::Library`]) or from an engine linked into the binary that
/// exposes its `duckdb_ext_api_v2` table - as the table every call of this crate goes through. A process binds one
/// engine: the first installation wins and later ones return `false` (the table in use may be the one an extension
/// loaded by the linked engine fetched through `get_api`, which is the same table for the same engine).
pub fn install_table(table: duckdb_ext_api_v2) -> bool {
    API.set(ApiTable(table)).is_ok()
}

/// A table filled from a symbol resolver (`dlsym` over a loaded `libduckdb`): entry `i` of the table is
/// `resolve(API_TABLE_FUNCTION_NAMES[i])`, a null pointer leaving the entry `None` (a library that lacks the
/// function, e.g. an upstream build without a newer engine's appended entries; a call through it is
/// `Error::missing_entry`).
pub fn table_from_symbols(mut resolve: impl FnMut(&str) -> *mut c_void) -> duckdb_ext_api_v2 {
    let mut table = std::mem::MaybeUninit::<duckdb_ext_api_v2>::zeroed();
    let slots = table.as_mut_ptr() as *mut *mut c_void;
    for (i, name) in API_TABLE_FUNCTION_NAMES.iter().enumerate() {
        let symbol = resolve(name);
        // SAFETY(ffi): L8 - `duckdb_ext_api_v2` is `#[repr(C)]` with one pointer-sized `Option<unsafe extern "C" fn>`
        // field per name of `API_TABLE_FUNCTION_NAMES`, in that order (asserted where both are generated), so slot
        // `i` is the field named `name`; a null pointer is `None` and a non-null one `Some` by the null-pointer
        // optimization of `Option<fn>`, and the symbol is the C function the header declares under that name.
        unsafe { slots.add(i).write(symbol) };
    }
    // SAFETY(ffi): every slot was written (a resolved symbol or the zero of `None`).
    unsafe { table.assume_init() }
}

/// The function table. Panics when the entrypoint has not initialized it: nothing of the API may be
/// used before `DUCKDB_EXTENSION_API_INIT`, so this is a programming error of the extension.
#[inline]
pub fn api() -> &'static duckdb_ext_api_v2 {
    match API.get() {
        Some(table) => &table.0,
        None => uninitialized_api(),
    }
}

#[cold]
#[inline(never)]
fn uninitialized_api() -> ! {
    panic!("duckdb-ext-api: the C API table is not initialized (the extension entrypoint has not run)")
}

/// Calls an entry of the table that takes a trailing error slot, converting the outcome into
/// [`crate::Result`]. A table entry the engine left null (an older engine) is an `API` error.
macro_rules! api_call {
    ($fn:ident ( $($arg:expr),* $(,)? )) => {{
        let f = $crate::api::api().$fn.ok_or_else(|| $crate::Error::missing_entry(stringify!($fn)))?;
        let mut err: $crate::sys::duckdb_v2_error_info_handle = ::std::ptr::null_mut();
        // SAFETY(ffi): the table entry is the engine's `duckdb_v2_*` function; the arguments follow its contract.
        let code = unsafe { f($($arg,)* &mut err) };
        $crate::error::check(code, err)?
    }};
}

/// Destroys an owned handle through its `*_destroy` entry, ignoring the outcome (destructors cannot fail).
macro_rules! api_destroy {
    ($fn:ident ( $slot:expr )) => {{
        if let Some(f) = $crate::api::api().$fn {
            // SAFETY(ffi): `$slot` is the owned handle's slot; destroy nulls it.
            let _ = unsafe { f($slot) };
        }
    }};
}

/// Calls an entry that returns a handle through its last out-parameter (before the error slot) and wraps the
/// handle with `$wrap`, one of this crate's `unsafe fn from_raw` / `borrowed` constructors:
/// `api_call_out!(duckdb_v2_vector_get_value(v, row) -> Value::from_raw)`.
macro_rules! api_call_out {
    ($fn:ident ( $($arg:expr),* $(,)? ) -> $wrap:path) => {{
        let mut raw = ::std::ptr::null_mut();
        api_call!($fn($($arg,)* &mut raw));
        // SAFETY(ffi): the engine filled the out-parameter with the handle the entry's contract describes - owned for a
        // `*_create` / `*_copy` / value-returning getter, lent for the duration of the callback for the infos'
        // accessors - and the constructor named at the call site is the one for that ownership.
        unsafe { $wrap(raw) }
    }};
}

/// A getter of an opaque slot (`user_data`, `bind_data`, `init_data`, ...) as `&T`: the slot holds what this crate
/// stored through [`crate::opaque::Opaque`] for the `T` the caller names.
macro_rules! opaque_getter {
    ($(#[$doc:meta])* $name:ident, $api:ident) => {
        $(#[$doc])*
        pub fn $name<T>(&self) -> $crate::Result<Option<&T>> {
            let mut data: *mut ::std::ffi::c_void = ::std::ptr::null_mut();
            api_call!($api(self.raw, &mut data));
            // SAFETY(ffi): the slot was filled by `Opaque::new` / `with_equality` with the `T` the caller names (the
            // function's own data), or is empty; the engine keeps it alive for the callback.
            Ok(unsafe { $crate::opaque::Opaque::get::<T>(data) })
        }
    };
}

/// A getter of an exclusive slot (a thread's local state, a scalar call's init data, a flush's batch data) as
/// `&'cb mut T`, handed out once per callback: the info carries a [`crate::opaque::CallbackSlots`] token named
/// `slots`, and `'cb` is the lifetime of the trampoline frame the info was created for. Invoked inside the
/// `impl<'cb> Info<'cb>` block of such an info.
macro_rules! exclusive_getter {
    ($(#[$doc:meta])* $name:ident, $api:ident, $slot:ident, $what:literal) => {
        $(#[$doc])*
        pub fn $name<T>(&self) -> $crate::Result<Option<&'cb mut T>> {
            self.slots.take_exclusive($crate::opaque::CallbackSlots::$slot, $what)?;
            let mut data: *mut ::std::ffi::c_void = ::std::ptr::null_mut();
            api_call!($api(self.raw, &mut data));
            // SAFETY(ffi L8.I10): the slot was filled by `Opaque::new` with the `T` the caller names, or is empty; the engine hands
            // it to one callback at a time (the header sentence cited at `CallbackSlots`) and keeps it alive for the
            // callback, `take_exclusive` handed it out once in this callback, and `'cb` ends with the trampoline frame:
            // no other reference to the value exists while this one lives.
            Ok(unsafe { $crate::opaque::Opaque::get_mut::<T>(data) })
        }
    };
}

/// An `extern "C"` callback of the `(info, context, err)` shape that runs `F::$method` over the wrapped info.
macro_rules! trampoline {
    ($name:ident, $trait:ident :: $method:ident, $info:ident, $handle:ty) => {
        unsafe extern "C" fn $name<F: $trait>(
            info: $handle,
            context: $crate::sys::duckdb_v2_context_handle,
            err: *mut $crate::sys::duckdb_v2_error_info_handle,
        ) {
            // SAFETY(ffi): the engine lends live handles for the duration of the callback; the wrappers are borrowed
            // for the call and never outlive it.
            $crate::error::guard(err, || unsafe {
                F::$method(&$info { raw: info }, &$crate::context::Context::from_raw(context))
            });
        }
    };
}

/// [`trampoline!`] for an info with exclusive slots ([`crate::opaque::CallbackSlots`]): the info is created with the
/// token of this frame, so the `&'cb mut T` its exclusive getters hand out cannot outlive the callback.
macro_rules! trampoline_cb {
    ($name:ident, $trait:ident :: $method:ident, $info:ident, $handle:ty) => {
        unsafe extern "C" fn $name<F: $trait>(
            info: $handle,
            context: $crate::sys::duckdb_v2_context_handle,
            err: *mut $crate::sys::duckdb_v2_error_info_handle,
        ) {
            // the exclusive slots are handed out for this frame only
            let scope = ();
            // SAFETY(ffi L8.I1): as for `trampoline!`.
            $crate::error::guard(err, || unsafe {
                F::$method(
                    &$info { raw: info, slots: $crate::opaque::CallbackSlots::scoped(&scope) },
                    &$crate::context::Context::from_raw(context),
                )
            });
        }
    };
}

/// An `extern "C"` callback of the `(bind, info, context, err)` shape (the bind-time callbacks).
macro_rules! bind_trampoline {
    ($name:ident, $trait:ident :: $method:ident, $info:ident, $handle:ty) => {
        unsafe extern "C" fn $name<F: $trait>(
            bind: $crate::sys::duckdb_v2_function_bind_info_handle,
            info: $handle,
            context: $crate::sys::duckdb_v2_context_handle,
            err: *mut $crate::sys::duckdb_v2_error_info_handle,
        ) {
            // SAFETY(ffi): as for `trampoline!`.
            $crate::error::guard(err, || unsafe {
                F::$method(
                    &$crate::function::BindInfo::from_raw(bind),
                    &$info { raw: info },
                    &$crate::context::Context::from_raw(context),
                )
            });
        }
    };
}

/// An `extern "C"` callback of the `(info, err)` shape (no context).
macro_rules! info_trampoline {
    ($name:ident, $trait:ident :: $method:ident, $info:ident, $handle:ty) => {
        unsafe extern "C" fn $name<F: $trait>(info: $handle, err: *mut $crate::sys::duckdb_v2_error_info_handle) {
            $crate::error::guard(err, || F::$method(&$info { raw: info }));
        }
    };
}

/// `duckdb_v2_library_version`: the engine's version string (`v<release>-dev<N>` for a development build of either
/// engine).
pub fn library_version() -> crate::Result<String> {
    let mut version = sys::duckdb_v2_str::EMPTY;
    api_call!(duckdb_v2_library_version(&mut version));
    Ok(crate::text::str_to_string(version))
}

/// `duckdb_v2_library_api_table_size`: the byte size of the function table the engine hands out. newer engines only
/// (the entry opens the appended band): `Error::missing_entry` for a library without it - which is also what an
/// extension sees when the engine did not announce its table size ([`duckdb_capi_announce_api_table_size`]).
pub fn library_api_table_size() -> crate::Result<u64> {
    let mut size: sys::idx_t = 0;
    api_call!(duckdb_v2_library_api_table_size(&mut size));
    Ok(size)
}

// kept: the API table copy by announced size (the extension ABI handshake, no SQL surface)
#[cfg(test)]
mod tests {
    use super::*;

    /// A table whose slot `i` holds the fake address `i + 1` (never called: `copy_table` only copies).
    fn numbered_table() -> duckdb_ext_api_v2 {
        let mut next = 0usize;
        table_from_symbols(|_| {
            next += 1;
            next as *mut c_void
        })
    }

    /// The pointer-sized slots of a table, as `table_from_symbols` writes them (L8: one slot per name, in order).
    fn slots(table: &duckdb_ext_api_v2) -> Vec<usize> {
        let count = std::mem::size_of::<duckdb_ext_api_v2>() / std::mem::size_of::<usize>();
        let base = table as *const duckdb_ext_api_v2 as *const usize;
        // SAFETY(test): `duckdb_ext_api_v2` is `#[repr(C)]` with `count` pointer-sized `Option<fn>` fields, each a
        // valid `usize` to read; `table` is live for the call.
        (0..count).map(|i| unsafe { base.add(i).read() }).collect()
    }

    #[test]
    fn copy_without_announcement_is_the_upstream_prefix() {
        let source = numbered_table();
        let prefix_slots = UPSTREAM_API_TABLE_SIZE / std::mem::size_of::<usize>();
        // SAFETY(test): `source` is a live table of the full Rust-side layout, readable in whole.
        let copy = unsafe { copy_table(&source, 0) };
        let copied = slots(&copy);
        assert_eq!(&copied[..prefix_slots], &slots(&source)[..prefix_slots]);
        assert!(copied[prefix_slots..].iter().all(|&slot| slot == 0), "the appended band stays NULL");
        assert!(copy.duckdb_v2_library_version.is_some());
        assert!(copy.duckdb_v2_library_api_table_size.is_none(), "the band's first entry is beyond the prefix");
        // an announcement below the prefix is no announcement
        // SAFETY(test): as above.
        let short = unsafe { copy_table(&source, UPSTREAM_API_TABLE_SIZE - 8) };
        assert_eq!(slots(&short), copied);
    }

    #[test]
    fn copy_follows_the_announced_size_and_never_exceeds_the_struct() {
        let source = numbered_table();
        let total = std::mem::size_of::<duckdb_ext_api_v2>();
        // the engine's own size: everything
        // SAFETY(test): `source` is a live table of the full Rust-side layout, readable in whole.
        let full = unsafe { copy_table(&source, total) };
        assert_eq!(slots(&full), slots(&source));
        assert!(full.duckdb_v2_library_api_table_size.is_some());
        // a newer engine with a bigger table: this struct's worth of it
        // SAFETY(test): as above (the size is clamped to this struct before anything is read).
        let bigger = unsafe { copy_table(&source, total + 64) };
        assert_eq!(slots(&bigger), slots(&source));
        // an older engine whose band ends after its first entry: that entry and nothing beyond
        // SAFETY(test): as above.
        let older = unsafe { copy_table(&source, UPSTREAM_API_TABLE_SIZE + std::mem::size_of::<usize>()) };
        let band_start = UPSTREAM_API_TABLE_SIZE / std::mem::size_of::<usize>();
        let copied = slots(&older);
        assert_eq!(copied[band_start], band_start + 1);
        assert!(copied[band_start + 1..].iter().all(|&slot| slot == 0));
        assert!(older.duckdb_v2_library_api_table_size.is_some());
        assert!(older.duckdb_v2_vector_default_cast.is_none());
    }

    #[test]
    fn the_loader_announcement_is_the_process_wide_size() {
        let before = announced_api_table_size();
        duckdb_capi_announce_api_table_size(4096);
        assert_eq!(announced_api_table_size(), 4096);
        duckdb_capi_announce_api_table_size(before as sys::idx_t);
        assert_eq!(announced_api_table_size(), before);
    }
}
