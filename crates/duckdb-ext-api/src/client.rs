//! The client side of the C API v2: what a program that *drives* an engine - the shell, the sqllogictest runner,
//! the benchmark runner - needs on top of the extension-side wrappers. The engine is reached through the same
//! process-wide function table as from an extension ([`mod@crate::api`]); a client binds it once, either to a
//! `libduckdb` loaded at run time ([`Library::open`]: `dlopen` + `dlsym` - `LoadLibraryW` + `GetProcAddress` on
//! Windows - over `API_TABLE_FUNCTION_NAMES`) or to the
//! engine linked into the binary ([`crate::api::install_table`] with the table that engine exposes).
//!
//! | C++ (`duckdb.hpp`)                     | here                                                              |
//! |----------------------------------------|-------------------------------------------------------------------|
//! | `DuckDB db(path, config)`              | [`Environment::new`] + [`Environment::create_instance`] (the      |
//! |                                        | `DBConfig` options as `(name, value)` text) + [`Database::attach`] |
//! | `Connection con(db)`                   | [`Database::connect`]; [`SharedConnection`] for a connection      |
//! |                                        | shared with a watchdog thread (`Connection::Interrupt`)           |
//! | `con.Query(sql)`                       | [`crate::Connection::query`] / `parse_statements` + `execute_statement` |
//! | `QueryResult::Fetch` (streaming)       | [`crate::QueryResult::fetch_chunk`] / [`crate::QueryResult::step`] |
//! | `con.Prepare(sql)->Execute(..)`        | [`crate::Connection::prepare`] + [`crate::PreparedStatement::execute`] |
//! | `ClientContext::Append` (`Appender`)   | [`crate::ColumnDataCollection::append`] + [`crate::Statement::add_collection`] |

#[cfg(unix)]
use std::ffi::c_int;
use std::ffi::{CStr, CString, c_char, c_void};
use std::sync::{Mutex, MutexGuard};

use crate::Result;
use crate::context::{Config, Connection, Factory, Instance};
use crate::sync::MutexExt;
use crate::sys::*;
use crate::text::str_arg;

// -------------------------------------------------------------------------------------------------
// Loading a library at run time
// -------------------------------------------------------------------------------------------------

#[cfg(unix)]
#[link(name = "dl")]
unsafe extern "C" {
    fn dlopen(filename: *const c_char, flag: c_int) -> *mut c_void;
    fn dlsym(handle: *mut c_void, symbol: *const c_char) -> *mut c_void;
    fn dlerror() -> *mut c_char;
}

/// `<dlfcn.h>`: `RTLD_NOW` is 2 everywhere; `RTLD_LOCAL` is 0 on glibc (the default) but 4 on Darwin, where the
/// default without it is `RTLD_GLOBAL` (dyld `include/dlfcn.h`).
#[cfg(unix)]
const RTLD_NOW: c_int = 2;
#[cfg(all(unix, target_vendor = "apple"))]
const RTLD_LOCAL: c_int = 4;
#[cfg(all(unix, not(target_vendor = "apple")))]
const RTLD_LOCAL: c_int = 0;

// The Windows loader (`dl.hpp`'s shims): `dlopen` is `LoadLibraryW`, `dlsym` is `GetProcAddress` (its `FARPROC`
// result is a pointer-sized function address, taken as a `*mut c_void` like `dlsym`'s).
#[cfg(windows)]
#[link(name = "kernel32")]
unsafe extern "system" {
    fn LoadLibraryW(file_name: *const u16) -> *mut c_void;
    fn GetProcAddress(module: *mut c_void, proc_name: *const c_char) -> *mut c_void;
}

/// `dlopen(path, RTLD_NOW | RTLD_LOCAL)` / `LoadLibraryW(path)`: the handle, null on failure.
fn load_library(cpath: &CStr) -> *mut c_void {
    #[cfg(unix)]
    // SAFETY(ffi): a valid C string; the flags are the documented RTLD constants.
    unsafe {
        dlopen(cpath.as_ptr(), RTLD_NOW | RTLD_LOCAL)
    }
    #[cfg(windows)]
    {
        let mut wpath: Vec<u16> = cpath.to_string_lossy().encode_utf16().collect();
        wpath.push(0);
        // SAFETY(ffi L8.I11): `wpath` is a NUL-terminated UTF-16 string.
        unsafe { LoadLibraryW(wpath.as_ptr()) }
    }
}

/// `dlsym(handle, name)` / `GetProcAddress(handle, name)`: null when the library has no such symbol.
fn load_symbol(handle: *mut c_void, cname: &CStr) -> *mut c_void {
    #[cfg(unix)]
    // SAFETY(ffi): `handle` is a live dlopen handle; `cname` is a valid C string.
    unsafe {
        dlsym(handle, cname.as_ptr())
    }
    #[cfg(windows)]
    // SAFETY(ffi L8.I11): `handle` is a live module handle; `cname` is a valid C string.
    unsafe {
        GetProcAddress(handle, cname.as_ptr())
    }
}

/// The last `dlerror()` text, or a generic message; on Windows the system text of `GetLastError()`.
fn dl_error() -> String {
    #[cfg(unix)]
    {
        // SAFETY(ffi): dlerror returns a static or thread-local C string, or NULL.
        let err = unsafe { dlerror() };
        if err.is_null() {
            return "unknown dynamic loading error".to_string();
        }
        // SAFETY(ffi): non-null C string owned by the loader.
        unsafe { CStr::from_ptr(err) }.to_string_lossy().into_owned()
    }
    #[cfg(windows)]
    {
        std::io::Error::last_os_error().to_string()
    }
}

/// A `libduckdb` loaded at run time whose entry points are the process's API table. Never unloaded: the engine may
/// keep background threads until the process exits, and the table keeps pointing into it.
pub struct Library {
    path: String,
}

/// The path [`Library::open`] bound the process to, for the later opens of the same path.
static OPENED: Mutex<Option<String>> = Mutex::new(None);

impl Library {
    /// `dlopen`s `path` (a file path, or a bare library name for the loader's search path), resolves every entry
    /// of `API_TABLE_FUNCTION_NAMES` with `dlsym` (a missing symbol leaves its entry `None`) and installs the table
    /// as the process's. Opening the path the process is bound to again is a no-op; opening another fails, as does
    /// a file that cannot be loaded or exports no `duckdb_v2_library_version` (not a DuckDB C API v2 library), and
    /// a process bound to the engine linked into it.
    pub fn open(path: &str) -> std::result::Result<Library, String> {
        let mut opened = OPENED.lock_ignore_poison();
        if let Some(current) = opened.as_deref() {
            if current == path {
                return Ok(Library { path: path.to_string() });
            }
            return Err(format!("the C API is already bound to {current}; cannot switch to {path}"));
        }
        if crate::api::is_api_initialized() {
            return Err(format!(
                "the C API is already bound to the engine linked into the process; cannot switch to {path}"
            ));
        }
        let cpath = CString::new(path).map_err(|_| "library path contains a NUL byte".to_string())?;
        let handle = load_library(&cpath);
        if handle.is_null() {
            return Err(format!("could not load {path}: {}", dl_error()));
        }
        let table = crate::api::table_from_symbols(|name| {
            let cname = CString::new(name).expect("symbol names have no NUL");
            load_symbol(handle, &cname)
        });
        if table.duckdb_v2_library_version.is_none() {
            return Err(format!("{path}: missing symbol duckdb_v2_library_version (not a DuckDB C API v2 library)"));
        }
        if !crate::api::install_table(table) {
            return Err(format!("the C API is already bound to an engine in this process; cannot switch to {path}"));
        }
        *opened = Some(path.to_string());
        Ok(Library { path: path.to_string() })
    }

    /// The path the library was opened from.
    pub fn path(&self) -> &str {
        &self.path
    }

    /// The path the process is bound to through [`Library::open`], `None` when no library was opened (the engine
    /// linked into the process, or nothing yet).
    pub fn opened_path() -> Option<String> {
        OPENED.lock_ignore_poison().clone()
    }

    /// The platform's dynamic library name of the engine: `libduckdb.so` (Linux), `libduckdb.dylib` (macOS),
    /// `duckdb.dll` (Windows).
    pub fn file_name() -> String {
        format!("{}duckdb{}", std::env::consts::DLL_PREFIX, std::env::consts::DLL_SUFFIX)
    }
}

// -------------------------------------------------------------------------------------------------
// Environment / instance
// -------------------------------------------------------------------------------------------------

/// The root `duckdb_v2_environment_handle` (owned): the registry every instance of a process is created in. The
/// API refuses to destroy an environment with live instances, so dropping one early leaks it rather than dangling.
pub struct Environment {
    raw: duckdb_v2_environment_handle,
}

// SAFETY(protocol L8.I8): (T5, L8.I1) the owned root `duckdb_v2_environment_handle` (`duckdb_v2.h`: "the required root
// through which instance handles are created"; the engine's environment object is an instance count plus the
// process-wide instance cache the engine locks), so the environment entries are callable from any thread; the wrapper
// holds nothing else and destroys the handle once, through `&mut self`.
unsafe impl Send for Environment {}
unsafe impl Sync for Environment {}

impl Environment {
    /// `duckdb_v2_environment_create`.
    pub fn new() -> Result<Environment> {
        let mut raw: duckdb_v2_environment_handle = std::ptr::null_mut();
        api_call!(duckdb_v2_environment_create(&mut raw));
        Ok(Environment { raw })
    }

    pub fn as_raw(&self) -> duckdb_v2_environment_handle {
        self.raw
    }

    /// `DuckDB db(nullptr, &config)` without a database: an instance configured with the startup `options` as
    /// `(name, value)` text pairs (`DBConfig::SetOptionByName`; `duckdb_v2_instance_create_with_options`). Attach the
    /// database file with [`Database::attach`].
    pub fn create_instance(&self, options: &[(&str, &str)]) -> Result<Database> {
        let names: Vec<duckdb_v2_str> = options.iter().map(|(n, _)| str_arg(n)).collect();
        let values: Vec<duckdb_v2_str> = options.iter().map(|(_, v)| str_arg(v)).collect();
        let mut raw: duckdb_v2_instance_handle = std::ptr::null_mut();
        api_call!(duckdb_v2_instance_create_with_options(
            self.raw,
            if names.is_empty() { std::ptr::null() } else { names.as_ptr() },
            if values.is_empty() { std::ptr::null() } else { values.as_ptr() },
            options.len() as idx_t,
            &mut raw
        ));
        Ok(Database { raw })
    }
}

impl Drop for Environment {
    fn drop(&mut self) {
        api_destroy!(duckdb_v2_environment_destroy(&mut self.raw));
    }
}

/// An owned database instance (`DuckDB`); [`Instance`] is its borrowed form. The connections opened on it keep the
/// engine-side instance alive, so dropping the `Database` before them is safe.
pub struct Database {
    raw: duckdb_v2_instance_handle,
}

// SAFETY(protocol L8.I8): (T5, L8.I1) an owned `duckdb_v2_instance_handle` (`duckdb_v2.h`: "a buffer pool, a scheduler,
// a set of GLOBAL settings, and the databases attached to it" - the engine's `Arc<DatabaseInstance>`, whose connect /
// attach / config entries take the database's own locks); callable from any thread, the wrapper
// holds nothing else.
unsafe impl Send for Database {}
unsafe impl Sync for Database {}

impl Database {
    /// Takes ownership of an instance handle the API returned.
    /// # Safety
    /// (ffi) `raw` must be a handle the C API returned for this type and that is still live, and nothing else may
    /// destroy it (the wrapper destroys it on drop).
    pub unsafe fn from_raw(raw: duckdb_v2_instance_handle) -> Database {
        Database { raw }
    }

    pub fn as_raw(&self) -> duckdb_v2_instance_handle {
        self.raw
    }

    /// The borrowed view ([`Instance::factory`], [`Instance::config`], ...).
    pub fn instance(&self) -> Instance {
        // SAFETY(ffi): T1 - the handle is owned by `self` and live; the view does not outlive it.
        unsafe { Instance::from_raw(self.raw) }
    }

    /// `ATTACH 'path' AS name` (`duckdb_v2_instance_attach`): an empty `name` derives it from the path, an empty
    /// path or `:memory:` attaches an in-memory database; `make_default` makes it the default database, which is
    /// what `DuckDB(path, config)` does with its argument.
    pub fn attach(&self, name: &str, path: &str, options: Option<&AttachOptions>, make_default: bool) -> Result<()> {
        let name = str_arg(name);
        let path = str_arg(path);
        api_call!(duckdb_v2_instance_attach(
            self.raw,
            &name,
            &path,
            options.map_or(std::ptr::null_mut(), |o| o.raw),
            make_default
        ));
        Ok(())
    }

    /// `Connection con(db)`.
    pub fn connect(&self) -> Result<Connection> {
        self.instance().connect()
    }

    pub fn factory(&self) -> Result<Factory> {
        self.instance().factory()
    }

    pub fn config(&self) -> Result<Config> {
        self.instance().config()
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        api_destroy!(duckdb_v2_instance_destroy(&mut self.raw));
    }
}

/// The options of an `ATTACH` (`AttachOptions`, owned): `READ_ONLY`, `TYPE`, `BLOCK_SIZE`, ... as text.
pub struct AttachOptions {
    raw: duckdb_v2_attach_options_handle,
}

impl AttachOptions {
    /// `duckdb_v2_attach_options_create`.
    pub fn new(instance: Instance) -> Result<AttachOptions> {
        let mut raw: duckdb_v2_attach_options_handle = std::ptr::null_mut();
        api_call!(duckdb_v2_attach_options_create(instance.as_raw(), &mut raw));
        Ok(AttachOptions { raw })
    }

    pub fn as_raw(&self) -> duckdb_v2_attach_options_handle {
        self.raw
    }

    /// Sets one option from its text (`duckdb_v2_attach_options_set`).
    pub fn set(&self, key: &str, value: &str) -> Result<()> {
        let key = str_arg(key);
        let value = str_arg(value);
        api_call!(duckdb_v2_attach_options_set(self.raw, &key, &value));
        Ok(())
    }
}

impl Drop for AttachOptions {
    fn drop(&mut self) {
        api_destroy!(duckdb_v2_attach_options_destroy(&mut self.raw));
    }
}

// -------------------------------------------------------------------------------------------------
// Connection: progress, interrupt, sharing across threads
// -------------------------------------------------------------------------------------------------

/// `QueryProgress` (`duckdb_v2_connection_progress_get`): `percentage` is -1 while no query is running.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct QueryProgress {
    pub percentage: f64,
    pub rows_processed: u64,
    pub total_rows_to_process: u64,
}

/// `duckdb_v2_connection_progress_get` on a raw handle (the entry is documented as safe from any thread).
fn progress_of(raw: duckdb_v2_connection_handle) -> Result<QueryProgress> {
    let mut progress = QueryProgress { percentage: -1.0, rows_processed: 0, total_rows_to_process: 0 };
    api_call!(duckdb_v2_connection_progress_get(
        raw,
        &mut progress.percentage,
        &mut progress.rows_processed,
        &mut progress.total_rows_to_process
    ));
    Ok(progress)
}

/// `duckdb_v2_connection_interrupt` on a raw handle (the entry is documented as safe from any thread).
fn interrupt_raw(raw: duckdb_v2_connection_handle) -> Result<()> {
    api_call!(duckdb_v2_connection_interrupt(raw));
    Ok(())
}

impl Connection {
    /// `ClientContext::GetQueryProgress`: the progress of the running query.
    pub fn progress(&self) -> Result<QueryProgress> {
        progress_of(self.as_raw())
    }
}

/// A connection shared between the thread that runs queries on it and a thread that may interrupt them (a
/// watchdog, a Ctrl-C handler): the queries go through [`SharedConnection::lock`], which serializes them the way
/// the engine requires (a connection is single-consumer), while [`SharedConnection::interrupt`] and
/// [`SharedConnection::progress`] take the two entries the C API documents as callable from any thread without the
/// lock, so they reach a connection in the middle of a query.
pub struct SharedConnection {
    raw: duckdb_v2_connection_handle,
    connection: Mutex<Connection>,
}

// SAFETY(protocol L8.I8): (T5, L8.I1) `connection` is reached only through the mutex (a connection is single-consumer,
// see `Connection`), and `raw` (the same handle, live as long as `connection` owns it) is only passed to
// `duckdb_v2_connection_interrupt` / `duckdb_v2_connection_progress_get`, which `duckdb_v2.h` documents as "safe to
// call from any thread ..., including while another thread steps the query's result".
unsafe impl Send for SharedConnection {}
unsafe impl Sync for SharedConnection {}

impl SharedConnection {
    pub fn new(connection: Connection) -> SharedConnection {
        SharedConnection { raw: connection.as_raw(), connection: Mutex::new(connection) }
    }

    /// The connection, for the duration of the guard (poison-tolerant: a panic in another holder does not take the
    /// connection down).
    pub fn lock(&self) -> MutexGuard<'_, Connection> {
        self.connection.lock_ignore_poison()
    }

    /// The raw handle, for the thread-safe entries only.
    pub fn as_raw(&self) -> duckdb_v2_connection_handle {
        self.raw
    }

    /// `Connection::Interrupt()`: cancels the statement running on the connection, from any thread.
    pub fn interrupt(&self) -> Result<()> {
        interrupt_raw(self.raw)
    }

    /// The progress of the running query, from any thread.
    pub fn progress(&self) -> Result<QueryProgress> {
        progress_of(self.raw)
    }
}
