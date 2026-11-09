//! Instance, connection, context, factory and config handles.

use crate::Result;
use crate::file_system::FileSystem;
use crate::sys::*;
use crate::text::{render, str_arg, str_to_string};
use crate::value::Value;

/// `DUCKDB_V2_LOG_LEVEL` (`DUCKDB_V2_LOG_LEVEL_TRACE` ... `FATAL`).
pub type LogLevel = DUCKDB_V2_LOG_LEVEL;
/// `DUCKDB_V2_SETTING_SCOPE` (`DEFAULT` / `GLOBAL` / `SESSION`).
pub type SettingScope = DUCKDB_V2_SETTING_SCOPE;

/// Creates types, values and function builders: every database object is created through the factory
/// of the instance it belongs to. Borrowed from an instance, connection or context.
#[derive(Clone, Copy)]
pub struct Factory {
    raw: duckdb_v2_factory_handle,
}

impl Factory {
    /// # Safety
    /// (ffi L8.I1) `raw` must be a handle the C API returned for this type and that is still live. An owned wrapper
    /// destroys the handle on drop, so nothing else may destroy it; a borrowed (`Copy`) wrapper must not be used past
    /// the call that lent the handle.
    pub unsafe fn from_raw(raw: duckdb_v2_factory_handle) -> Factory {
        Factory { raw }
    }

    pub fn as_raw(&self) -> duckdb_v2_factory_handle {
        self.raw
    }
}

/// The context an extension entrypoint or a callback runs under (borrowed; a transaction is active).
#[derive(Clone, Copy)]
pub struct Context {
    raw: duckdb_v2_context_handle,
}

impl Context {
    /// # Safety
    /// (ffi L8.I1) `raw` must be a handle the C API returned for this type and that is still live. An owned wrapper
    /// destroys the handle on drop, so nothing else may destroy it; a borrowed (`Copy`) wrapper must not be used past
    /// the call that lent the handle.
    #[inline]
    pub unsafe fn from_raw(raw: duckdb_v2_context_handle) -> Context {
        Context { raw }
    }

    #[inline]
    pub fn as_raw(&self) -> duckdb_v2_context_handle {
        self.raw
    }

    #[inline]
    pub fn factory(&self) -> Result<Factory> {
        let mut raw = std::ptr::null_mut();
        api_call!(duckdb_v2_context_get_factory(self.raw, &mut raw));
        Ok(Factory { raw })
    }

    pub fn config(&self) -> Result<Config> {
        let mut raw: duckdb_v2_config_handle = std::ptr::null_mut();
        api_call!(duckdb_v2_context_get_config(self.raw, &mut raw));
        Ok(Config { raw })
    }

    /// The effective value of a setting as seen from this context (`ClientContext::TryGetCurrentSetting`): `None`
    /// for an option the engine does not know.
    pub fn setting(&self, name: &str) -> Result<Option<Value>> {
        Ok(self.config()?.get(name).ok().map(|(value, _)| value))
    }

    /// `MetaTransaction::Get(context).start_timestamp`: the start of the context's transaction in microseconds since
    /// the epoch - what `now()` and `current_date` report. newer engines only (`None` for a library without the entry).
    pub fn transaction_timestamp(&self) -> Result<Option<i64>> {
        if crate::api::api().duckdb_v2_context_get_transaction_timestamp.is_none() {
            return Ok(None);
        }
        let mut micros: i64 = 0;
        api_call!(duckdb_v2_context_get_transaction_timestamp(self.raw, &mut micros));
        Ok(Some(micros))
    }

    /// The client's file system (`file_search_path`, `enable_external_access` and the registered
    /// sub-file-systems apply).
    /// `BufferAllocator::Get(context).Allocate(byte_len)`: a block of memory tracked against the database's
    /// `memory_limit` (an Out of Memory Error when the limit does not allow it), the way the built-in readers hold
    /// their read buffers. `None` when the loading library has no tracked allocations (the upstream C API).
    pub fn allocate_data(&self, byte_len: usize) -> Result<Option<AllocatedData>> {
        if crate::api::api().duckdb_v2_context_allocate_data.is_none() {
            return Ok(None);
        }
        let mut raw: duckdb_v2_allocated_data_handle = std::ptr::null_mut();
        api_call!(duckdb_v2_context_allocate_data(self.raw, byte_len as idx_t, &mut raw));
        Ok(Some(AllocatedData::from_raw(raw)?))
    }

    pub fn file_system(&self) -> Result<FileSystem> {
        Ok(api_call_out!(duckdb_v2_context_get_file_system(self.raw) -> FileSystem::from_raw))
    }

    /// `CoordinateReferenceSystem::TryConvert`: a coordinate reference system definition in another format, through
    /// the coordinate systems the catalog knows; `None` when it cannot provide one.
    pub fn crs_convert(&self, definition: &str, target: CrsType) -> Result<Option<String>> {
        let mut converted = false;
        let text = render(|buffer, capacity, needed| {
            let def = str_arg(definition);
            api_call!(duckdb_v2_context_crs_convert(
                self.raw,
                &def,
                target.to_raw(),
                buffer,
                capacity,
                needed,
                &mut converted
            ));
            Ok(())
        })?;
        Ok(converted.then_some(text))
    }

    /// `CoordinateReferenceSystem::TryIdentify`: the shortest form the catalog knows a coordinate reference system
    /// as (what binding `GEOMETRY(crs)` stores); `None` for an incomplete definition the catalog does not know.
    pub fn crs_identify(&self, definition: &str) -> Result<Option<String>> {
        let mut identified = false;
        let text = render(|buffer, capacity, needed| {
            let def = str_arg(definition);
            api_call!(duckdb_v2_context_crs_identify(self.raw, &def, buffer, capacity, needed, &mut identified));
            Ok(())
        })?;
        Ok(identified.then_some(text))
    }

    /// Logs under the connection scope (`duckdb_logs`).
    pub fn log(&self, level: LogLevel, log_type: &str, message: &str) -> Result<()> {
        let log_type = str_arg(log_type);
        let message = str_arg(message);
        api_call!(duckdb_v2_context_log(self.raw, level, &log_type, &message));
        Ok(())
    }

    /// Whether `log(level, log_type, ..)` would record anything (`Logger::ShouldLog`).
    pub fn should_log(&self, level: LogLevel, log_type: &str) -> Result<bool> {
        let log_type = str_arg(log_type);
        let mut should_log = false;
        api_call!(duckdb_v2_context_should_log(self.raw, level, &log_type, &mut should_log));
        Ok(should_log)
    }
}

/// A database instance (borrowed).
#[derive(Clone, Copy)]
pub struct Instance {
    raw: duckdb_v2_instance_handle,
}

impl Instance {
    /// # Safety
    /// (ffi L8.I1) `raw` must be a handle the C API returned for this type and that is still live. An owned wrapper
    /// destroys the handle on drop, so nothing else may destroy it; a borrowed (`Copy`) wrapper must not be used past
    /// the call that lent the handle.
    pub unsafe fn from_raw(raw: duckdb_v2_instance_handle) -> Instance {
        Instance { raw }
    }

    pub fn as_raw(&self) -> duckdb_v2_instance_handle {
        self.raw
    }

    pub fn factory(&self) -> Result<Factory> {
        let mut raw = std::ptr::null_mut();
        api_call!(duckdb_v2_instance_get_factory(self.raw, &mut raw));
        Ok(Factory { raw })
    }

    pub fn config(&self) -> Result<Config> {
        let mut raw: duckdb_v2_config_handle = std::ptr::null_mut();
        api_call!(duckdb_v2_instance_get_config(self.raw, &mut raw));
        Ok(Config { raw })
    }

    /// The database's virtual file system (no client sandboxing).
    pub fn file_system(&self) -> Result<FileSystem> {
        Ok(api_call_out!(duckdb_v2_instance_get_file_system(self.raw) -> FileSystem::from_raw))
    }

    /// Opens a connection.
    pub fn connect(&self) -> Result<Connection> {
        let mut raw = std::ptr::null_mut();
        api_call!(duckdb_v2_connection_create(self.raw, &mut raw));
        Ok(Connection { raw, owned: true })
    }

    /// Logs under the database scope.
    pub fn log(&self, level: LogLevel, log_type: &str, message: &str) -> Result<()> {
        let log_type = str_arg(log_type);
        let message = str_arg(message);
        api_call!(duckdb_v2_instance_log(self.raw, level, &log_type, &message));
        Ok(())
    }
}

/// A connection: owned when opened through [`Instance::connect`], borrowed when wrapped with
/// [`Connection::borrowed`].
pub struct Connection {
    raw: duckdb_v2_connection_handle,
    owned: bool,
}

// SAFETY(protocol L8.I8): (T5, L8.I1) an owned or borrowed `duckdb_v2_connection_handle` (`duckdb_v2.h`: "a session on
// a database instance with its own LOCAL settings and transaction state"). The session is single-consumer (its results
// are "single consumer: step from one thread at a time"), so the wrapper is `Send` - a session moves to another thread
// as a whole (a borrowed one only within the callback that lent it) - and not `Sync`; `client::SharedConnection` adds
// the lock and the two entries the header allows from any thread.
unsafe impl Send for Connection {}

impl Connection {
    /// # Safety
    /// (ffi L8.I1) `raw` must be a handle the C API returned for this type and that is still live. An owned wrapper
    /// destroys the handle on drop, so nothing else may destroy it; a borrowed (`Copy`) wrapper must not be used past
    /// the call that lent the handle.
    pub unsafe fn from_raw(raw: duckdb_v2_connection_handle) -> Connection {
        Connection { raw, owned: true }
    }

    /// # Safety
    /// (ffi L8.I1) `raw` must be a handle the C API returned for this type and that is still live. An owned wrapper
    /// destroys the handle on drop, so nothing else may destroy it; a borrowed (`Copy`) wrapper must not be used past
    /// the call that lent the handle.
    pub unsafe fn borrowed(raw: duckdb_v2_connection_handle) -> Connection {
        Connection { raw, owned: false }
    }

    pub fn as_raw(&self) -> duckdb_v2_connection_handle {
        self.raw
    }

    pub fn factory(&self) -> Result<Factory> {
        let mut raw: duckdb_v2_factory_handle = std::ptr::null_mut();
        api_call!(duckdb_v2_connection_get_factory(self.raw, &mut raw));
        Ok(Factory { raw })
    }

    pub fn config(&self) -> Result<Config> {
        let mut raw: duckdb_v2_config_handle = std::ptr::null_mut();
        api_call!(duckdb_v2_connection_get_config(self.raw, &mut raw));
        Ok(Config { raw })
    }

    pub fn file_system(&self) -> Result<FileSystem> {
        Ok(api_call_out!(duckdb_v2_connection_get_file_system(self.raw) -> FileSystem::from_raw))
    }

    pub fn log(&self, level: LogLevel, log_type: &str, message: &str) -> Result<()> {
        let log_type = str_arg(log_type);
        let message = str_arg(message);
        api_call!(duckdb_v2_connection_log(self.raw, level, &log_type, &message));
        Ok(())
    }

    /// Interrupts the running query.
    pub fn interrupt(&self) -> Result<()> {
        api_call!(duckdb_v2_connection_interrupt(self.raw));
        Ok(())
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        if self.owned {
            api_destroy!(duckdb_v2_connection_destroy(&mut self.raw));
        }
    }
}

/// The settings of an instance / connection (borrowed).
#[derive(Clone, Copy)]
pub struct Config {
    raw: duckdb_v2_config_handle,
}

impl Config {
    pub fn as_raw(&self) -> duckdb_v2_config_handle {
        self.raw
    }

    /// The current value of a setting and the scope it is set in.
    pub fn get(&self, name: &str) -> Result<(Value, SettingScope)> {
        let name = str_arg(name);
        let mut value: duckdb_v2_value_handle = std::ptr::null_mut();
        let mut scope = DUCKDB_V2_SETTING_SCOPE_DEFAULT;
        api_call!(duckdb_v2_config_get_option_value(self.raw, &name, &mut value, &mut scope));
        // SAFETY(ffi L8.I1): the entry returns an owned copy of the option's value.
        Ok((unsafe { Value::from_raw(value) }, scope))
    }

    /// Sets a setting from its text (`SET name = 'text'`).
    pub fn set_text(&self, name: &str, text: &str, scope: SettingScope) -> Result<()> {
        let name = str_arg(name);
        let text = str_arg(text);
        api_call!(duckdb_v2_config_set_option_text(self.raw, &name, &text, scope));
        Ok(())
    }

    /// Sets a setting from a value.
    pub fn set(&self, name: &str, value: &Value, scope: SettingScope) -> Result<()> {
        let name = str_arg(name);
        api_call!(duckdb_v2_config_set_option_value(self.raw, &name, value.as_raw(), scope));
        Ok(())
    }

    /// The names of all known settings.
    pub fn option_names(&self) -> Result<Vec<String>> {
        let mut count: idx_t = 0;
        api_call!(duckdb_v2_config_get_option_count(self.raw, &mut count));
        let mut names = Vec::with_capacity(count as usize);
        for i in 0..count {
            let mut option: duckdb_v2_option_handle = std::ptr::null_mut();
            api_call!(duckdb_v2_config_get_option_by_index(self.raw, i, &mut option));
            let mut name = duckdb_v2_str::EMPTY;
            let outcome: Result<()> = (|| {
                api_call!(duckdb_v2_option_get_name(option, &mut name));
                Ok(())
            })();
            let text = str_to_string(name);
            api_destroy!(duckdb_v2_option_destroy(&mut option));
            outcome?;
            names.push(text);
        }
        Ok(names)
    }
}

/// `CoordinateReferenceSystemType`: the format of a coordinate reference system definition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CrsType {
    Invalid,
    Srid,
    AuthCode,
    ProjJson,
    Wkt2_2019,
}

impl CrsType {
    pub fn to_raw(self) -> DUCKDB_V2_CRS_TYPE {
        match self {
            CrsType::Invalid => DUCKDB_V2_CRS_TYPE_INVALID,
            CrsType::Srid => DUCKDB_V2_CRS_TYPE_SRID,
            CrsType::AuthCode => DUCKDB_V2_CRS_TYPE_AUTH_CODE,
            CrsType::ProjJson => DUCKDB_V2_CRS_TYPE_PROJJSON,
            CrsType::Wkt2_2019 => DUCKDB_V2_CRS_TYPE_WKT2_2019,
        }
    }

    pub fn from_raw(raw: DUCKDB_V2_CRS_TYPE) -> CrsType {
        match raw {
            DUCKDB_V2_CRS_TYPE_SRID => CrsType::Srid,
            DUCKDB_V2_CRS_TYPE_AUTH_CODE => CrsType::AuthCode,
            DUCKDB_V2_CRS_TYPE_PROJJSON => CrsType::ProjJson,
            DUCKDB_V2_CRS_TYPE_WKT2_2019 => CrsType::Wkt2_2019,
            _ => CrsType::Invalid,
        }
    }
}

/// A parsed coordinate reference system definition (`CoordinateReferenceSystem(definition)`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedCrs {
    pub kind: CrsType,
    /// `GetIdentifier()`: the definition itself for `AUTHORITY:CODE` / SRID, the `id` of a PROJJSON / WKT2 document
    pub identifier: String,
}

/// Parses a coordinate reference system definition without a catalog; malformed WKT2 is an error.
pub fn parse_crs(definition: &str) -> Result<ParsedCrs> {
    let mut kind = DUCKDB_V2_CRS_TYPE_INVALID;
    let identifier = render(|buffer, capacity, needed| {
        let def = str_arg(definition);
        api_call!(duckdb_v2_crs_parse(&def, &mut kind, buffer, capacity, needed));
        Ok(())
    })?;
    Ok(ParsedCrs { kind: CrsType::from_raw(kind), identifier })
}

/// An owned block of the database's buffer allocator (`AllocatedData`), or of its buffer pool
/// ([`BufferAllocator::allocate_buffer`]): counts towards `memory_limit` until it is dropped. The bytes are
/// uninitialized; [`append`](Self::append) fills them front to back (or [`fill`](Self::fill) fills the whole block)
/// and [`initialized`](Self::initialized) / [`initialized_mut`](Self::initialized_mut) read and write the filled
/// prefix, the one safe way to read the block.
pub struct AllocatedData {
    raw: duckdb_v2_allocated_data_handle,
    ptr: *mut u8,
    size: usize,
    /// The bytes `[0, initialized)` were written through `append` or `fill` (the only writers that advance it).
    initialized: usize,
}

// SAFETY(protocol L8.I8): (T5, L8.I1) an owned `duckdb_v2_allocated_data_handle` over plain memory (`duckdb_v2.h`: the
// block "does not depend on the context it was allocated through" and is "valid until the handle is destroyed"). The
// safe API reads the wrapper (`len`) and the prefix `append` / `fill` wrote (`initialized`, `initialized_mut`) and
// writes the bytes through `&mut self` (`as_mut_slice`, `append`, `fill`); `as_mut_ptr` hands out the raw pointer,
// whose every dereference is the caller's own `unsafe` with its own exclusivity argument (the json reader's
// `BufferHandle`), so sharing `&AllocatedData` gives no safe mutation.
unsafe impl Send for AllocatedData {}
unsafe impl Sync for AllocatedData {}

impl AllocatedData {
    /// Takes ownership of a freshly allocated handle and reads its pointer and size.
    fn from_raw(raw: duckdb_v2_allocated_data_handle) -> Result<AllocatedData> {
        // `data` owns the block from here on: an early return destroys it exactly once (it is filled in place rather
        // than through a struct update, which would copy the `Copy` handle and drop - destroy - the original)
        let mut data = AllocatedData { raw, ptr: std::ptr::null_mut(), size: 0, initialized: 0 };
        api_call!(duckdb_v2_allocated_data_get_pointer(raw, &mut data.ptr));
        let mut size: idx_t = 0;
        api_call!(duckdb_v2_allocated_data_get_size(raw, &mut size));
        data.size = size as usize;
        Ok(data)
    }

    pub fn as_mut_ptr(&self) -> *mut u8 {
        self.ptr
    }

    /// The block's bytes, uninitialized until written (`duckdb_v2.h`: "The block is uninitialized").
    pub fn as_mut_slice(&mut self) -> &mut [std::mem::MaybeUninit<u8>] {
        // SAFETY(ffi L8.I1): `ptr` addresses `size` bytes the engine's allocator handed out for the life of the handle
        // (L3/L4 owners), viewed as uninitialized bytes; `&mut self` makes this the one writer.
        unsafe { std::slice::from_raw_parts_mut(self.ptr as *mut std::mem::MaybeUninit<u8>, self.size) }
    }

    /// Appends `bytes` after the initialized prefix (`memcpy(data.get() + size, bytes, len)` of the C++ callers).
    /// Panics when the bytes do not fit: the callers reserve first (`CachedFileDownload::Append`).
    pub fn append(&mut self, bytes: &[u8]) {
        let start = self.initialized;
        let end = start.checked_add(bytes.len()).filter(|&end| end <= self.size).expect("append beyond the block");
        self.as_mut_slice()[start..end].write_copy_of_slice(bytes);
        self.initialized = end;
    }

    /// Writes `byte` into every byte of the block (`memset` of the C++ callers that zero a buffer): the whole block
    /// is initialized afterwards.
    pub fn fill(&mut self, byte: u8) {
        self.as_mut_slice().fill(std::mem::MaybeUninit::new(byte));
        self.initialized = self.size;
    }

    /// The prefix [`append`](Self::append) / [`fill`](Self::fill) filled.
    pub fn initialized(&self) -> &[u8] {
        // SAFETY(ffi L8.I1): `ptr` addresses `size` bytes the engine's allocator handed out for the life of the handle
        // (as `as_mut_slice`); the first `initialized` of them were written through `append` / `fill` (the field's
        // invariant: they are its only writers and advance it only over bytes they just wrote), and `&self` excludes a
        // concurrent writer (every writer needs `&mut self`).
        unsafe { std::slice::from_raw_parts(self.ptr, self.initialized) }
    }

    /// The prefix [`append`](Self::append) / [`fill`](Self::fill) filled, writable (the bytes stay initialized).
    pub fn initialized_mut(&mut self) -> &mut [u8] {
        // SAFETY(ffi L8.I1): as `initialized`; `&mut self` makes this the one writer.
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.initialized) }
    }

    pub fn initialized_len(&self) -> usize {
        self.initialized
    }

    pub fn len(&self) -> usize {
        self.size
    }

    pub fn is_empty(&self) -> bool {
        self.size == 0
    }
}

impl Drop for AllocatedData {
    fn drop(&mut self) {
        api_destroy!(duckdb_v2_allocated_data_destroy(&mut self.raw));
    }
}

/// The tracked allocator of an opener's database (`BufferAllocator::Get(context)`, the C++ `HTTPFileHandle::
/// buffer_allocator`), owned: a file handle keeps it for the reads and writes that follow its open. Its blocks count
/// towards `memory_limit` and the buffer manager evicts to make room for them; an opener without a database yields
/// the untracked default allocator (`Allocator::DefaultAllocator()`), as in the C++.
pub struct BufferAllocator {
    raw: duckdb_v2_buffer_allocator_handle,
}

// SAFETY(protocol L8.I8): (T5, L8.I1) an owned `duckdb_v2_buffer_allocator_handle` (`duckdb_v2.h`: "may be kept
// beyond the callback it was obtained in"; the engine's wrapper holds an `Arc<dyn BufferManager>` whose allocator is
// thread-safe), callable from any thread; the wrapper holds nothing else and destroys the handle once, through `&mut
// self`.
unsafe impl Send for BufferAllocator {}
unsafe impl Sync for BufferAllocator {}

impl BufferAllocator {
    /// Takes ownership of the handle an entry returned.
    pub(crate) fn from_raw(raw: duckdb_v2_buffer_allocator_handle) -> BufferAllocator {
        BufferAllocator { raw }
    }

    /// `Allocator::Allocate(byte_len)`: a block counted under the `ALLOCATOR` tag of `duckdb_memory()`; the buffer
    /// manager's Out of Memory Error (`failed to allocate data of size ...`) when the limit does not allow it.
    pub fn allocate(&self, byte_len: usize) -> Result<AllocatedData> {
        let mut raw: duckdb_v2_allocated_data_handle = std::ptr::null_mut();
        api_call!(duckdb_v2_buffer_allocator_allocate(self.raw, byte_len as idx_t, &mut raw));
        AllocatedData::from_raw(raw)
    }

    /// `BufferManager::Allocate(MemoryTag::EXTENSION, byte_len)`: a pinned block of the buffer pool counted under the
    /// `EXTENSION` tag (the C++ S3 upload parts); `could not allocate block of size ...` when the limit does not allow
    /// it. The block may be larger than asked.
    pub fn allocate_buffer(&self, byte_len: usize) -> Result<AllocatedData> {
        let mut raw: duckdb_v2_allocated_data_handle = std::ptr::null_mut();
        api_call!(duckdb_v2_buffer_allocator_allocate_buffer(self.raw, byte_len as idx_t, &mut raw));
        AllocatedData::from_raw(raw)
    }
}

impl Drop for BufferAllocator {
    fn drop(&mut self) {
        api_destroy!(duckdb_v2_buffer_allocator_destroy(&mut self.raw));
    }
}
