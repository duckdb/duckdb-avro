//! File systems an extension implements (`duckdb_v2_custom_file_system_*`): a `FileSystem` subsystem registered
//! with the engine's virtual file system, the open request / file opener / file list sink / logger views the
//! callbacks receive, and `duckdb_v2_file_system_path_canonicalize`. The httpfs extension's `HTTPFileSystem`,
//! `S3FileSystem` and `HuggingFaceFileSystem` are built on this (see the engine's C API additions).
//!
//! An extension implements [`CustomFileSystem`] (the path-level operations and `open`) and [`CustomFile`] (the
//! operations on an open file) and hands an instance to [`CustomFileSystemBuilder::set_implementation`], which
//! stores it as the file system's user data and registers a trampoline for every callback. Operations the
//! implementation does not override report a Not Implemented error, like the C++ `FileSystem` base class. The engine
//! may call the file operations of one handle from several threads at once (`PARALLEL_ACCESS`), so a [`CustomFile`]
//! uses interior mutability for its position and buffers.

use std::ffi::c_void;

use crate::context::{BufferAllocator, Context, Factory, LogLevel};
use crate::error::guard;
use crate::file_system::{FileSystem, collect_text};
use crate::opaque::Opaque;
use crate::sys::*;
use crate::text::{bytes_arg, str_arg, str_to_string};
use crate::value::Value;
use crate::{Error, Result};

/// `DUCKDB_V2_FILE_WRITE_MODE` (`SEQUENTIAL` / `CONCURRENT_SEQUENTIAL` / `POSITIONAL`).
pub type FileWriteMode = DUCKDB_V2_FILE_WRITE_MODE;

/// The open flags of a request (`duckdb_v2_file_open_request_get_flags()`): a mask of the `DUCKDB_V2_FILE_OPEN_FLAGS_*`
/// bits (`FileOpenFlags::FILE_FLAGS_*` of the engine).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FileOpenFlagsMask(pub idx_t);

impl FileOpenFlagsMask {
    fn has(self, flag: DUCKDB_V2_FILE_OPEN_FLAGS) -> bool {
        self.0 & flag.0 as idx_t != 0
    }

    pub fn read(self) -> bool {
        self.has(DUCKDB_V2_FILE_OPEN_FLAGS_READ)
    }

    pub fn write(self) -> bool {
        self.has(DUCKDB_V2_FILE_OPEN_FLAGS_WRITE)
    }

    pub fn direct_io(self) -> bool {
        self.has(DUCKDB_V2_FILE_OPEN_FLAGS_DIRECT_IO)
    }

    pub fn create(self) -> bool {
        self.has(DUCKDB_V2_FILE_OPEN_FLAGS_FILE_CREATE)
    }

    pub fn create_new(self) -> bool {
        self.has(DUCKDB_V2_FILE_OPEN_FLAGS_FILE_CREATE_NEW)
    }

    pub fn append(self) -> bool {
        self.has(DUCKDB_V2_FILE_OPEN_FLAGS_APPEND)
    }

    pub fn private(self) -> bool {
        self.has(DUCKDB_V2_FILE_OPEN_FLAGS_PRIVATE)
    }

    /// A missing file is reported with `OpenRequest::set_not_found` (an `open` returning `None`) instead of an error.
    pub fn null_if_not_exists(self) -> bool {
        self.has(DUCKDB_V2_FILE_OPEN_FLAGS_NULL_IF_NOT_EXISTS)
    }

    pub fn parallel_access(self) -> bool {
        self.has(DUCKDB_V2_FILE_OPEN_FLAGS_PARALLEL_ACCESS)
    }

    pub fn exclusive_create(self) -> bool {
        self.has(DUCKDB_V2_FILE_OPEN_FLAGS_EXCLUSIVE_CREATE)
    }

    pub fn null_if_exists(self) -> bool {
        self.has(DUCKDB_V2_FILE_OPEN_FLAGS_NULL_IF_EXISTS)
    }

    pub fn multi_client_access(self) -> bool {
        self.has(DUCKDB_V2_FILE_OPEN_FLAGS_MULTI_CLIENT_ACCESS)
    }

    pub fn disable_logging(self) -> bool {
        self.has(DUCKDB_V2_FILE_OPEN_FLAGS_DISABLE_LOGGING)
    }

    pub fn enable_extension_install(self) -> bool {
        self.has(DUCKDB_V2_FILE_OPEN_FLAGS_ENABLE_EXTENSION_INSTALL)
    }
}

/// A measured network throughput estimate (`NetworkThroughputEstimate`): the time to first byte in seconds and the
/// single-stream bandwidth in bytes per second.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct NetworkThroughput {
    pub latency_seconds: f64,
    pub bandwidth_bytes_per_second: f64,
}

fn not_implemented(what: &str) -> Error {
    Error::not_implemented(format!("{what} is not implemented for this file system"))
}

// -------------------------------------------------------------------------------------------------
// The implementation traits
// -------------------------------------------------------------------------------------------------

/// An open file of a [`CustomFileSystem`] (the per-file data of the C API). Every operation has the C++ `FileSystem`
/// base behaviour as its default.
pub trait CustomFile: Send + Sync {
    /// `FileSystem::Read(handle, buffer, nr_bytes, location)`: exactly `buffer.len()` bytes at `location`.
    fn read_at(&self, _buffer: &mut [u8], _location: u64) -> Result<()> {
        Err(not_implemented("Read (with location)"))
    }

    /// `FileSystem::Read(handle, buffer, nr_bytes)`: up to `buffer.len()` bytes at the position, advancing it.
    fn read(&self, _buffer: &mut [u8]) -> Result<usize> {
        Err(not_implemented("Read"))
    }

    /// `FileSystem::Write(handle, buffer, nr_bytes, location)`.
    fn write_at(&self, _buffer: &[u8], _location: u64) -> Result<()> {
        Err(not_implemented("Write (with location)"))
    }

    /// `FileSystem::Write(handle, buffer, nr_bytes)`.
    fn write(&self, _buffer: &[u8]) -> Result<usize> {
        Err(not_implemented("Write"))
    }

    /// `FileSystem::FileSync`: what finalizes an upload.
    fn sync(&self) -> Result<()> {
        Err(not_implemented("FileSync"))
    }

    /// `FileSystem::AbortFileWrite`: a writer gives up an incomplete file.
    fn abort_write(&self) -> Result<()> {
        Err(not_implemented("AbortFileWrite"))
    }

    /// `FileHandle::Close`, run before the file is dropped.
    fn close(&self) -> Result<()> {
        Ok(())
    }

    /// `FileSystem::GetFileSize`.
    fn size(&self) -> Result<i64> {
        Err(not_implemented("GetFileSize"))
    }

    /// `FileSystem::GetLastModifiedTime`: microseconds since the Unix epoch.
    fn last_modified(&self) -> Result<i64> {
        Err(not_implemented("GetLastModifiedTime"))
    }

    /// `FileSystem::GetCacheValidUntil`: the freshness deadline in microseconds since the epoch, if the server gave
    /// one.
    fn cache_valid_until(&self) -> Result<Option<i64>> {
        Ok(None)
    }

    /// `FileSystem::GetVersionTag`: the ETag (empty when unknown).
    fn version_tag(&self) -> Result<Vec<u8>> {
        Ok(Vec::new())
    }

    /// `FileSystem::Seek`.
    fn seek(&self, _location: u64) -> Result<()> {
        Err(not_implemented("Seek"))
    }

    /// `FileSystem::SeekPosition`.
    fn position(&self) -> Result<u64> {
        Err(not_implemented("SeekPosition"))
    }

    /// `FileSystem::OnDiskFile`.
    fn on_disk(&self) -> bool {
        true
    }

    /// `FileSystem::TryGetNetworkThroughput`: the estimate the prefetch cost model uses, once there is one.
    fn network_throughput(&self) -> Option<NetworkThroughput> {
        None
    }
}

/// A file system an extension implements. `can_handle` and `open` are what a file system needs; the other operations
/// default to the C++ `FileSystem` base behaviour (a Not Implemented error; `remove_files` one by one).
pub trait CustomFileSystem: Send + Sync + 'static {
    /// `FileSystem::CanHandleFile`: whether the path is routed to this file system (by its prefix).
    fn can_handle(&self, path: &str) -> bool;

    /// `FileSystem::OpenFileExtended`. `Ok(None)` reports a missing (or existing) file when the request's flags ask
    /// for a NULL handle in that case (`null_if_not_exists` / `null_if_exists`); it is an error otherwise.
    fn open(&self, request: &OpenRequest) -> Result<Option<Box<dyn CustomFile>>>;

    /// `FileSystem::FileExists`.
    fn file_exists(&self, _path: &str, _opener: Option<FileOpener>) -> Result<bool> {
        Err(not_implemented("FileExists"))
    }

    /// `FileSystem::DirectoryExists`.
    fn directory_exists(&self, _path: &str, _opener: Option<FileOpener>) -> Result<bool> {
        Err(not_implemented("DirectoryExists"))
    }

    /// `FileSystem::RemoveFile`.
    fn remove_file(&self, _path: &str, _opener: Option<FileOpener>) -> Result<()> {
        Err(not_implemented("RemoveFile"))
    }

    /// `FileSystem::RemoveFiles`: a bulk delete; a file that does not exist is not an error. The default removes the
    /// files one by one (`TryRemoveFile`).
    fn remove_files(&self, paths: &[String], opener: Option<FileOpener>) -> Result<()> {
        for path in paths {
            if self.file_exists(path, opener)? {
                self.remove_file(path, opener)?;
            }
        }
        Ok(())
    }

    /// `FileSystem::RemoveDirectory`.
    fn remove_directory(&self, _path: &str, _opener: Option<FileOpener>) -> Result<()> {
        Err(not_implemented("RemoveDirectory"))
    }

    /// `FileSystem::Glob`: the files matching `pattern`, added to `sink` (with the extended information a later open
    /// can use instead of a HEAD request).
    fn glob(&self, _pattern: &str, _opener: Option<FileOpener>, _sink: &FileListSink) -> Result<()> {
        Err(not_implemented("Glob"))
    }

    /// `FileSystem::ListFiles`: the entries of a directory (a directory entry carries the option `type` =
    /// `directory`).
    fn list_files(&self, _directory: &str, _opener: Option<FileOpener>, _sink: &FileListSink) -> Result<()> {
        Err(not_implemented("ListFiles"))
    }
}

// -------------------------------------------------------------------------------------------------
// Trampolines
// -------------------------------------------------------------------------------------------------
//
// The contract of every trampoline below (T4): the engine calls a trampoline only
// through the callback slot `set_implementation::<F>` registered it in, with the file system's user data
// (`fs_data`: the `F` stored by `set_implementation`), the per-file data (`file_data`: the `Box<dyn CustomFile>`
// stored by `OpenRequest::set_file`, passed to the operations of that handle until `close`), and arguments valid for
// the duration of the call - borrowed handles (the request, opener and sink), `duckdb_v2_str` views, byte buffers of
// the stated size and out-parameters (T3). The helpers below state those facts once; a trampoline's SAFETY cites them.
// Panics never cross the ABI: `guard` reports them through the error slot, the two `bool` callbacks catch them.

/// # Safety
/// (ffi) `fs_data` is the user data `set_implementation::<F>` stored for the file system this callback belongs to (the
/// engine passes it to every callback of that file system for the system's life; L8.I1).
unsafe fn fs_of<'a, F: CustomFileSystem>(fs_data: *mut c_void) -> &'a F {
    // SAFETY(ffi): forwarded - `set_implementation` stored an `F` as the file system's user data.
    unsafe { Opaque::get::<F>(fs_data) }.expect("a custom file system callback ran without its implementation")
}

/// # Safety
/// (ffi) `file_data` is the per-file data `OpenRequest::set_file` stored (a `Box<dyn CustomFile>`), which the engine
/// passes to the operations of that handle until `close` (L8.I1).
unsafe fn file_of<'a>(file_data: *mut c_void) -> &'a dyn CustomFile {
    // SAFETY(ffi): forwarded - `OpenRequest::set_file` stored a `Box<dyn CustomFile>` as the per-file data.
    let file = unsafe { Opaque::get::<Box<dyn CustomFile>>(file_data) }.expect("a file callback ran without its file");
    file.as_ref()
}

/// # Safety
/// (ffi) `opener` is null or the file-opener handle the engine lent for the current callback; the view is not used past
/// the call (L8.I1).
unsafe fn opener_of(opener: duckdb_v2_file_opener_handle) -> Option<FileOpener> {
    if opener.is_null() { None } else { Some(FileOpener { raw: opener }) }
}

/// # Safety
/// (ffi) T3: `path` is null or points at a `duckdb_v2_str` (and the bytes it views) valid for the duration of the call.
unsafe fn path_of(path: *const duckdb_v2_str) -> String {
    if path.is_null() {
        return String::new();
    }
    // SAFETY(ffi): the `# Safety` contract above (T3): `path` is non-null here and points at a `duckdb_v2_str` valid
    // for the call; `str_to_string` copies the viewed bytes before returning.
    str_to_string(unsafe { *path })
}

/// # Safety
/// (ffi) T3: `buffer` is null or valid for `size` readable bytes for `'a` (the duration of the call), not written to
/// meanwhile.
unsafe fn slice_of<'a>(buffer: *const c_void, size: idx_t) -> &'a [u8] {
    if buffer.is_null() || size == 0 {
        return &[];
    }
    // SAFETY(ffi): the `# Safety` contract above (T3): `buffer` is non-null here and valid for `size` readable bytes
    // for `'a`, not written meanwhile.
    unsafe { std::slice::from_raw_parts(buffer as *const u8, size as usize) }
}

/// # Safety
/// (ffi) T3: `buffer` is null or valid for `size` writable bytes for `'a` (the duration of the call), reached through
/// no other reference meanwhile.
unsafe fn slice_of_mut<'a>(buffer: *mut c_void, size: idx_t) -> &'a mut [u8] {
    if buffer.is_null() || size == 0 {
        return &mut [];
    }
    // SAFETY(ffi): the `# Safety` contract above (T3): `buffer` is non-null here and valid for `size` writable bytes
    // for `'a`, reached through no other reference meanwhile.
    unsafe { std::slice::from_raw_parts_mut(buffer as *mut u8, size as usize) }
}

#[expect(
    clippy::disallowed_methods,
    reason = "an FFI callback boundary: a Rust panic must not unwind into the engine's C caller (it is reported as the callback's failure)"
)]
unsafe extern "C" fn can_handle_trampoline<F: CustomFileSystem>(
    fs_data: *mut c_void,
    path: *const duckdb_v2_str,
) -> bool {
    // SAFETY(ffi): T4 - `fs_data` is this file system's `F` (`fs_of`) and `path` the call's argument (`path_of`).
    std::panic::catch_unwind(|| unsafe { fs_of::<F>(fs_data).can_handle(&path_of(path)) }).unwrap_or(false)
}

unsafe extern "C" fn open_trampoline<F: CustomFileSystem>(
    fs_data: *mut c_void,
    request: duckdb_v2_file_open_request_handle,
    err: *mut duckdb_v2_error_info_handle,
) {
    guard(err, || {
        // the request handle is lent for the duration of the callback (L8.I1)
        let request = OpenRequest { raw: request };
        // SAFETY(ffi): T4 - `fs_data` is this file system's `F` (`fs_of`).
        match unsafe { fs_of::<F>(fs_data) }.open(&request)? {
            Some(file) => request.set_file(file),
            None => request.set_not_found(),
        }
    });
}

unsafe extern "C" fn read_at_trampoline(
    _fs_data: *mut c_void,
    file_data: *mut c_void,
    buffer: *mut c_void,
    size: idx_t,
    location: idx_t,
    err: *mut duckdb_v2_error_info_handle,
) {
    // SAFETY(ffi): T4 - `file_data` is this handle's file (`file_of`); `buffer` is the engine's `size`-byte buffer for
    // the call (`slice_of_mut`, T3).
    guard(err, || unsafe { file_of(file_data).read_at(slice_of_mut(buffer, size), location) });
}

unsafe extern "C" fn read_trampoline(
    _fs_data: *mut c_void,
    file_data: *mut c_void,
    buffer: *mut c_void,
    size: idx_t,
    out_bytes_read: *mut idx_t,
    err: *mut duckdb_v2_error_info_handle,
) {
    guard(err, || {
        // SAFETY(ffi): T4 - `file_data` is this handle's file (`file_of`); `buffer` is the engine's `size`-byte buffer
        // and `out_bytes_read` its out-parameter for the call (T3).
        unsafe {
            let read = file_of(file_data).read(slice_of_mut(buffer, size))?;
            if !out_bytes_read.is_null() {
                *out_bytes_read = read as idx_t;
            }
        }
        Ok(())
    });
}

unsafe extern "C" fn write_at_trampoline(
    _fs_data: *mut c_void,
    file_data: *mut c_void,
    buffer: *const c_void,
    size: idx_t,
    location: idx_t,
    err: *mut duckdb_v2_error_info_handle,
) {
    // SAFETY(ffi): T4 - `file_data` is this handle's file (`file_of`); `buffer` is the engine's `size`-byte buffer for
    // the call (`slice_of`, T3).
    guard(err, || unsafe { file_of(file_data).write_at(slice_of(buffer, size), location) });
}

unsafe extern "C" fn write_trampoline(
    _fs_data: *mut c_void,
    file_data: *mut c_void,
    buffer: *const c_void,
    size: idx_t,
    out_bytes_written: *mut idx_t,
    err: *mut duckdb_v2_error_info_handle,
) {
    guard(err, || {
        // SAFETY(ffi): T4 - `file_data` is this handle's file (`file_of`); `buffer` is the engine's `size`-byte buffer
        // and `out_bytes_written` its out-parameter for the call (T3).
        unsafe {
            let written = file_of(file_data).write(slice_of(buffer, size))?;
            if !out_bytes_written.is_null() {
                *out_bytes_written = written as idx_t;
            }
        }
        Ok(())
    });
}

unsafe extern "C" fn sync_trampoline(
    _fs_data: *mut c_void,
    file_data: *mut c_void,
    err: *mut duckdb_v2_error_info_handle,
) {
    // SAFETY(ffi): T4 - `file_data` is this handle's file (`file_of`).
    guard(err, || unsafe { file_of(file_data) }.sync());
}

unsafe extern "C" fn abort_write_trampoline(
    _fs_data: *mut c_void,
    file_data: *mut c_void,
    err: *mut duckdb_v2_error_info_handle,
) {
    // SAFETY(ffi): T4 - `file_data` is this handle's file (`file_of`).
    guard(err, || unsafe { file_of(file_data) }.abort_write());
}

unsafe extern "C" fn close_trampoline(
    _fs_data: *mut c_void,
    file_data: *mut c_void,
    err: *mut duckdb_v2_error_info_handle,
) {
    // SAFETY(ffi): T4 - `file_data` is this handle's file (`file_of`); the engine releases the per-file data after
    // `close`.
    guard(err, || unsafe { file_of(file_data) }.close());
}

unsafe extern "C" fn size_trampoline(
    _fs_data: *mut c_void,
    file_data: *mut c_void,
    out_size: *mut i64,
    err: *mut duckdb_v2_error_info_handle,
) {
    guard(err, || {
        // SAFETY(ffi): T4 - `file_data` is this handle's file (`file_of`); `out_size` is the engine's out-parameter for
        // the call (T3).
        unsafe {
            let size = file_of(file_data).size()?;
            if !out_size.is_null() {
                *out_size = size;
            }
        }
        Ok(())
    });
}

unsafe extern "C" fn last_modified_trampoline(
    _fs_data: *mut c_void,
    file_data: *mut c_void,
    out_is_set: *mut bool,
    out_micros: *mut i64,
    err: *mut duckdb_v2_error_info_handle,
) {
    guard(err, || {
        // SAFETY(ffi): T4 - `file_data` is this handle's file (`file_of`); `out_is_set` and `out_micros` are the
        // engine's out-parameters for the call, always passed (T3).
        unsafe {
            let micros = file_of(file_data).last_modified()?;
            *out_is_set = true;
            *out_micros = micros;
        }
        Ok(())
    });
}

unsafe extern "C" fn cache_valid_until_trampoline(
    _fs_data: *mut c_void,
    file_data: *mut c_void,
    out_is_set: *mut bool,
    out_micros: *mut i64,
    err: *mut duckdb_v2_error_info_handle,
) {
    guard(err, || {
        // SAFETY(ffi): T4 - `file_data` is this handle's file (`file_of`); `out_is_set` and `out_micros` are the
        // engine's out-parameters for the call, always passed (T3).
        unsafe {
            let deadline = file_of(file_data).cache_valid_until()?;
            *out_is_set = deadline.is_some();
            *out_micros = deadline.unwrap_or(0);
        }
        Ok(())
    });
}

unsafe extern "C" fn version_tag_trampoline(
    _fs_data: *mut c_void,
    file_data: *mut c_void,
    sink: duckdb_v2_text_sink_fn,
    sink_data: *mut c_void,
    err: *mut duckdb_v2_error_info_handle,
) {
    guard(err, || {
        // SAFETY(ffi): T4 - `file_data` is this handle's file (`file_of`).
        let tag = unsafe { file_of(file_data) }.version_tag()?;
        if let Some(sink) = sink {
            let view = bytes_arg(&tag);
            // SAFETY(ffi): T4 - `sink` / `sink_data` are the engine's text sink for this call; it receives the bytes in
            // one call and copies them before returning.
            unsafe { sink(&view, sink_data, err) };
        }
        Ok(())
    });
}

unsafe extern "C" fn seek_trampoline(
    _fs_data: *mut c_void,
    file_data: *mut c_void,
    location: idx_t,
    err: *mut duckdb_v2_error_info_handle,
) {
    // SAFETY(ffi): T4 - `file_data` is this handle's file (`file_of`).
    guard(err, || unsafe { file_of(file_data) }.seek(location));
}

unsafe extern "C" fn position_trampoline(
    _fs_data: *mut c_void,
    file_data: *mut c_void,
    out_position: *mut idx_t,
    err: *mut duckdb_v2_error_info_handle,
) {
    guard(err, || {
        // SAFETY(ffi): T4 - `file_data` is this handle's file (`file_of`); `out_position` is the engine's out-parameter
        // for the call (T3).
        unsafe {
            let position = file_of(file_data).position()?;
            if !out_position.is_null() {
                *out_position = position;
            }
        }
        Ok(())
    });
}

#[expect(
    clippy::disallowed_methods,
    reason = "an FFI callback boundary: a Rust panic must not unwind into the engine's C caller (it is reported as the callback's failure)"
)]
unsafe extern "C" fn on_disk_trampoline(_fs_data: *mut c_void, file_data: *mut c_void) -> bool {
    // SAFETY(ffi): T4 - `file_data` is this handle's file (`file_of`).
    std::panic::catch_unwind(|| unsafe { file_of(file_data) }.on_disk()).unwrap_or(true)
}

#[expect(
    clippy::disallowed_methods,
    reason = "an FFI callback boundary: a Rust panic must not unwind into the engine's C caller (it is reported as the callback's failure)"
)]
unsafe extern "C" fn network_throughput_trampoline(
    _fs_data: *mut c_void,
    file_data: *mut c_void,
    out_latency_seconds: *mut f64,
    out_bandwidth_bytes_per_second: *mut f64,
) -> bool {
    // SAFETY(ffi): T4 - `file_data` is this handle's file (`file_of`).
    let estimate = std::panic::catch_unwind(|| unsafe { file_of(file_data) }.network_throughput()).unwrap_or(None);
    match estimate {
        Some(estimate) => {
            // SAFETY(ffi): T3 - the two out-parameters are the engine's for this call, always passed.
            unsafe {
                *out_latency_seconds = estimate.latency_seconds;
                *out_bandwidth_bytes_per_second = estimate.bandwidth_bytes_per_second;
            }
            true
        }
        None => false,
    }
}

unsafe extern "C" fn file_exists_trampoline<F: CustomFileSystem>(
    fs_data: *mut c_void,
    path: *const duckdb_v2_str,
    opener: duckdb_v2_file_opener_handle,
    out_result: *mut bool,
    err: *mut duckdb_v2_error_info_handle,
) {
    guard(err, || {
        // SAFETY(ffi): T4 - `fs_data` is this file system's `F` (`fs_of`), `path` and `opener` the call's arguments
        // (`path_of`, `opener_of`) and `out_result` the engine's out-parameter, always passed (T3).
        unsafe {
            let exists = fs_of::<F>(fs_data).file_exists(&path_of(path), opener_of(opener))?;
            *out_result = exists;
        }
        Ok(())
    });
}

unsafe extern "C" fn directory_exists_trampoline<F: CustomFileSystem>(
    fs_data: *mut c_void,
    path: *const duckdb_v2_str,
    opener: duckdb_v2_file_opener_handle,
    out_result: *mut bool,
    err: *mut duckdb_v2_error_info_handle,
) {
    guard(err, || {
        // SAFETY(ffi): T4 - `fs_data` is this file system's `F` (`fs_of`), `path` and `opener` the call's arguments
        // (`path_of`, `opener_of`) and `out_result` the engine's out-parameter, always passed (T3).
        unsafe {
            let exists = fs_of::<F>(fs_data).directory_exists(&path_of(path), opener_of(opener))?;
            *out_result = exists;
        }
        Ok(())
    });
}

unsafe extern "C" fn remove_file_trampoline<F: CustomFileSystem>(
    fs_data: *mut c_void,
    path: *const duckdb_v2_str,
    opener: duckdb_v2_file_opener_handle,
    err: *mut duckdb_v2_error_info_handle,
) {
    // SAFETY(ffi): T4 - `fs_data` is this file system's `F` (`fs_of`), `path` and `opener` the call's arguments
    // (`path_of`, `opener_of`).
    guard(err, || unsafe { fs_of::<F>(fs_data).remove_file(&path_of(path), opener_of(opener)) });
}

unsafe extern "C" fn remove_files_trampoline<F: CustomFileSystem>(
    fs_data: *mut c_void,
    paths: *const duckdb_v2_str,
    count: idx_t,
    opener: duckdb_v2_file_opener_handle,
    err: *mut duckdb_v2_error_info_handle,
) {
    guard(err, || {
        let views = if paths.is_null() || count == 0 {
            &[][..]
        } else {
            // SAFETY(ffi): T3 - `paths` points at `count` `duckdb_v2_str` views valid for the duration of the call.
            unsafe { std::slice::from_raw_parts(paths, count as usize) }
        };
        let paths: Vec<String> = views.iter().map(|view| str_to_string(*view)).collect();
        // SAFETY(ffi): T4 - `fs_data` is this file system's `F` (`fs_of`), `opener` the call's argument (`opener_of`).
        unsafe { fs_of::<F>(fs_data).remove_files(&paths, opener_of(opener)) }
    });
}

unsafe extern "C" fn remove_directory_trampoline<F: CustomFileSystem>(
    fs_data: *mut c_void,
    path: *const duckdb_v2_str,
    opener: duckdb_v2_file_opener_handle,
    err: *mut duckdb_v2_error_info_handle,
) {
    // SAFETY(ffi): T4 - `fs_data` is this file system's `F` (`fs_of`), `path` and `opener` the call's arguments
    // (`path_of`, `opener_of`).
    guard(err, || unsafe { fs_of::<F>(fs_data).remove_directory(&path_of(path), opener_of(opener)) });
}

unsafe extern "C" fn glob_trampoline<F: CustomFileSystem>(
    fs_data: *mut c_void,
    path: *const duckdb_v2_str,
    opener: duckdb_v2_file_opener_handle,
    sink: duckdb_v2_file_list_sink_handle,
    err: *mut duckdb_v2_error_info_handle,
) {
    // SAFETY(ffi): T4 - `fs_data` is this file system's `F` (`fs_of`), `path` and `opener` the call's arguments
    // (`path_of`, `opener_of`), `sink` the file-list sink the engine lends for the call (L8.I1).
    guard(err, || unsafe { fs_of::<F>(fs_data).glob(&path_of(path), opener_of(opener), &FileListSink { raw: sink }) });
}

unsafe extern "C" fn list_files_trampoline<F: CustomFileSystem>(
    fs_data: *mut c_void,
    path: *const duckdb_v2_str,
    opener: duckdb_v2_file_opener_handle,
    sink: duckdb_v2_file_list_sink_handle,
    err: *mut duckdb_v2_error_info_handle,
) {
    // SAFETY(ffi): T4 - `fs_data` is this file system's `F` (`fs_of`), `path` and `opener` the call's arguments
    // (`path_of`, `opener_of`), `sink` the file-list sink the engine lends for the call (L8.I1).
    guard(err, || unsafe {
        fs_of::<F>(fs_data).list_files(&path_of(path), opener_of(opener), &FileListSink { raw: sink })
    });
}

// -------------------------------------------------------------------------------------------------
// The builder
// -------------------------------------------------------------------------------------------------

/// A custom file system under construction (owned; registered with `Extension::register_file_system`).
pub struct CustomFileSystemBuilder {
    raw: duckdb_v2_custom_file_system_handle,
}

impl CustomFileSystemBuilder {
    /// `name` is `FileSystem::GetName()`: what `duckdb_logs` and `disabled_filesystems` refer to.
    pub fn new(factory: Factory, name: &str) -> Result<CustomFileSystemBuilder> {
        let name = str_arg(name);
        let mut raw: duckdb_v2_custom_file_system_handle = std::ptr::null_mut();
        api_call!(duckdb_v2_custom_file_system_create(factory.as_raw(), &name, &mut raw));
        Ok(CustomFileSystemBuilder { raw })
    }

    pub fn as_raw(&self) -> duckdb_v2_custom_file_system_handle {
        self.raw
    }

    /// `FileSystem::CanSeek()` of the handles (the default is false).
    pub fn set_can_seek(&self, can_seek: bool) -> Result<&Self> {
        api_call!(duckdb_v2_custom_file_system_set_can_seek(self.raw, can_seek));
        Ok(self)
    }

    /// `FileSystem::GetWriteMode()` of the handles (the default is SEQUENTIAL).
    pub fn set_write_mode(&self, mode: FileWriteMode) -> Result<&Self> {
        api_call!(duckdb_v2_custom_file_system_set_write_mode(self.raw, mode));
        Ok(self)
    }

    /// Stores `file_system` as the user data and registers every callback of [`CustomFileSystem`] / [`CustomFile`].
    pub fn set_implementation<F: CustomFileSystem>(&self, file_system: F) -> Result<&Self> {
        let mut opaque = Opaque::new(file_system);
        api_call!(duckdb_v2_custom_file_system_set_user_data(self.raw, &mut opaque));
        api_call!(duckdb_v2_custom_file_system_set_can_handle_file_callback(
            self.raw,
            Some(can_handle_trampoline::<F>)
        ));
        api_call!(duckdb_v2_custom_file_system_set_open_callback(self.raw, Some(open_trampoline::<F>)));
        api_call!(duckdb_v2_custom_file_system_set_read_at_callback(self.raw, Some(read_at_trampoline)));
        api_call!(duckdb_v2_custom_file_system_set_read_callback(self.raw, Some(read_trampoline)));
        api_call!(duckdb_v2_custom_file_system_set_write_at_callback(self.raw, Some(write_at_trampoline)));
        api_call!(duckdb_v2_custom_file_system_set_write_callback(self.raw, Some(write_trampoline)));
        api_call!(duckdb_v2_custom_file_system_set_file_sync_callback(self.raw, Some(sync_trampoline)));
        api_call!(duckdb_v2_custom_file_system_set_abort_file_write_callback(self.raw, Some(abort_write_trampoline)));
        api_call!(duckdb_v2_custom_file_system_set_close_callback(self.raw, Some(close_trampoline)));
        api_call!(duckdb_v2_custom_file_system_set_get_file_size_callback(self.raw, Some(size_trampoline)));
        api_call!(duckdb_v2_custom_file_system_set_get_last_modified_time_callback(
            self.raw,
            Some(last_modified_trampoline)
        ));
        api_call!(duckdb_v2_custom_file_system_set_get_cache_valid_until_callback(
            self.raw,
            Some(cache_valid_until_trampoline)
        ));
        api_call!(duckdb_v2_custom_file_system_set_get_version_tag_callback(self.raw, Some(version_tag_trampoline)));
        api_call!(duckdb_v2_custom_file_system_set_seek_callback(self.raw, Some(seek_trampoline)));
        api_call!(duckdb_v2_custom_file_system_set_seek_position_callback(self.raw, Some(position_trampoline)));
        api_call!(duckdb_v2_custom_file_system_set_on_disk_file_callback(self.raw, Some(on_disk_trampoline)));
        api_call!(duckdb_v2_custom_file_system_set_network_throughput_callback(
            self.raw,
            Some(network_throughput_trampoline)
        ));
        api_call!(duckdb_v2_custom_file_system_set_file_exists_callback(self.raw, Some(file_exists_trampoline::<F>)));
        api_call!(duckdb_v2_custom_file_system_set_directory_exists_callback(
            self.raw,
            Some(directory_exists_trampoline::<F>)
        ));
        api_call!(duckdb_v2_custom_file_system_set_remove_file_callback(self.raw, Some(remove_file_trampoline::<F>)));
        api_call!(duckdb_v2_custom_file_system_set_remove_files_callback(self.raw, Some(remove_files_trampoline::<F>)));
        api_call!(duckdb_v2_custom_file_system_set_remove_directory_callback(
            self.raw,
            Some(remove_directory_trampoline::<F>)
        ));
        api_call!(duckdb_v2_custom_file_system_set_glob_callback(self.raw, Some(glob_trampoline::<F>)));
        api_call!(duckdb_v2_custom_file_system_set_list_files_callback(self.raw, Some(list_files_trampoline::<F>)));
        Ok(self)
    }
}

impl Drop for CustomFileSystemBuilder {
    fn drop(&mut self) {
        api_destroy!(duckdb_v2_custom_file_system_destroy(&mut self.raw));
    }
}

// -------------------------------------------------------------------------------------------------
// The views the callbacks receive
// -------------------------------------------------------------------------------------------------

/// The arguments of an `open` (`duckdb_v2_file_open_request_handle`; borrowed for the duration of the callback).
#[derive(Clone, Copy)]
pub struct OpenRequest {
    raw: duckdb_v2_file_open_request_handle,
}

impl OpenRequest {
    /// # Safety
    /// (ffi) `raw` is the request handle the engine lent to an `open` callback; the view is not used past that call
    /// (L8.I1).
    pub unsafe fn from_raw(raw: duckdb_v2_file_open_request_handle) -> OpenRequest {
        OpenRequest { raw }
    }

    pub fn as_raw(&self) -> duckdb_v2_file_open_request_handle {
        self.raw
    }

    /// The path being opened.
    pub fn path(&self) -> Result<String> {
        let mut out: Vec<String> = Vec::new();
        api_call!(duckdb_v2_file_open_request_get_path(
            self.raw,
            Some(collect_text),
            &mut out as *mut Vec<String> as *mut c_void
        ));
        Ok(out.pop().unwrap_or_default())
    }

    /// The open flags.
    pub fn flags(&self) -> Result<FileOpenFlagsMask> {
        let mut flags: idx_t = 0;
        api_call!(duckdb_v2_file_open_request_get_flags(self.raw, &mut flags));
        Ok(FileOpenFlagsMask(flags))
    }

    /// An extended open option (`last_modified`, `etag`, `file_size`, `force_full_download`, ...) as a glob or a
    /// reader attached it; `None` when the request carries no such option.
    pub fn option(&self, name: &str) -> Result<Option<Value>> {
        let name = str_arg(name);
        let mut raw: duckdb_v2_value_handle = std::ptr::null_mut();
        api_call!(duckdb_v2_file_open_request_get_option(self.raw, &name, &mut raw));
        // SAFETY(ffi): the entry returns a copy of the value owned by the caller (null without the option); the owned
        // wrapper destroys it.
        Ok(if raw.is_null() { None } else { Some(unsafe { Value::from_raw(raw) }) })
    }

    /// The opener of the request (`None` when the engine opened without one).
    pub fn opener(&self) -> Result<Option<FileOpener>> {
        let mut raw: duckdb_v2_file_opener_handle = std::ptr::null_mut();
        api_call!(duckdb_v2_file_open_request_get_opener(self.raw, &mut raw));
        // SAFETY(ffi): the entry returns the opener lent for the duration of the callback this request belongs to (or
        // null).
        Ok(unsafe { opener_of(raw) })
    }

    /// Whether cached data must be validated against the remote file (`validate_external_file_cache`).
    pub fn cache_validation_mode(&self) -> Result<bool> {
        let mut validate = true;
        api_call!(duckdb_v2_file_open_request_get_cache_validation_mode(self.raw, &mut validate));
        Ok(validate)
    }

    /// Completes the open with `file` (what an `open` returning `Some` does).
    pub fn set_file(&self, file: Box<dyn CustomFile>) -> Result<()> {
        let mut opaque = Opaque::new(file);
        api_call!(duckdb_v2_file_open_request_set_file(self.raw, &mut opaque));
        Ok(())
    }

    /// Completes the open without a handle: the file does not exist (or exists) and the flags ask for a NULL handle
    /// in that case (what an `open` returning `None` does). Fails when the flags do not allow it.
    pub fn set_not_found(&self) -> Result<()> {
        api_call!(duckdb_v2_file_open_request_set_not_found(self.raw));
        Ok(())
    }
}

/// The `FileOpener` of a file system operation (borrowed for the duration of the callback): the client's settings,
/// context, user agent and logger.
#[derive(Clone, Copy)]
pub struct FileOpener {
    raw: duckdb_v2_file_opener_handle,
}

impl FileOpener {
    /// # Safety
    /// (ffi) `raw` is a file-opener handle the engine lent to the current callback; the view is not used past that call
    /// (L8.I1).
    pub unsafe fn from_raw(raw: duckdb_v2_file_opener_handle) -> FileOpener {
        FileOpener { raw }
    }

    pub fn as_raw(&self) -> duckdb_v2_file_opener_handle {
        self.raw
    }

    /// `FileOpener::TryGetCurrentSetting(opener, key, value, info)`: the current value of a setting in the opener's
    /// scope, `None` when the opener does not know it. `path` is the file the setting is read for.
    pub fn try_get_setting(&self, name: &str, path: Option<&str>) -> Result<Option<Value>> {
        let name = str_arg(name);
        let path_view = path.map(str_arg);
        let path_ptr = path_view.as_ref().map(|view| view as *const duckdb_v2_str).unwrap_or(std::ptr::null());
        let mut raw: duckdb_v2_value_handle = std::ptr::null_mut();
        api_call!(duckdb_v2_file_opener_try_get_setting(self.raw, &name, path_ptr, &mut raw));
        // SAFETY(ffi): the entry returns a copy of the value owned by the caller (null for an unknown setting); the
        // owned wrapper destroys it.
        Ok(if raw.is_null() { None } else { Some(unsafe { Value::from_raw(raw) }) })
    }

    /// The client the operation runs for (`None` for the database's own opener). Borrowed: valid for the duration of
    /// the callback.
    pub fn context(&self) -> Result<Option<Context>> {
        let mut raw: duckdb_v2_context_handle = std::ptr::null_mut();
        api_call!(duckdb_v2_file_opener_get_context(self.raw, &mut raw));
        // SAFETY(ffi): the entry returns a borrowed context valid for the duration of the callback (or null); `Context`
        // is the borrowed (`Copy`) wrapper.
        Ok(if raw.is_null() { None } else { Some(unsafe { Context::from_raw(raw) }) })
    }

    /// The `User-Agent` the opener's database sends (empty for an opener without a database).
    pub fn user_agent(&self) -> Result<String> {
        let mut out: Vec<String> = Vec::new();
        api_call!(duckdb_v2_file_opener_get_user_agent(
            self.raw,
            Some(collect_text),
            &mut out as *mut Vec<String> as *mut c_void
        ));
        Ok(out.pop().unwrap_or_default())
    }

    /// The logger of the opener's client, else the global logger of its database; `None` without either. Owned: it
    /// may be kept beyond the callback (a file handle logs through the logger it was opened with).
    pub fn logger(&self) -> Result<Option<Logger>> {
        let mut raw: duckdb_v2_logger_handle = std::ptr::null_mut();
        api_call!(duckdb_v2_file_opener_get_logger(self.raw, &mut raw));
        Ok(if raw.is_null() { None } else { Some(Logger { raw }) })
    }

    /// The tracked allocator of the opener's database (`BufferAllocator::Get(context)`, the default allocator for an
    /// opener without a database), owned: a file handle keeps it beyond the open for the buffers of its reads and
    /// writes. `None` when the loading library has no tracked allocations (the upstream C API).
    pub fn buffer_allocator(&self) -> Result<Option<BufferAllocator>> {
        if crate::api::api().duckdb_v2_file_opener_get_buffer_allocator.is_none() {
            return Ok(None);
        }
        let mut raw: duckdb_v2_buffer_allocator_handle = std::ptr::null_mut();
        api_call!(duckdb_v2_file_opener_get_buffer_allocator(self.raw, &mut raw));
        Ok(Some(BufferAllocator::from_raw(raw)))
    }
}

/// The result sink of `glob` / `list_files` (borrowed for the duration of the callback).
#[derive(Clone, Copy)]
pub struct FileListSink {
    raw: duckdb_v2_file_list_sink_handle,
}

impl FileListSink {
    /// # Safety
    /// (ffi) `raw` is the sink handle the engine lent to a `glob` / `list_files` callback; the view is not used past
    /// that call (L8.I1).
    pub unsafe fn from_raw(raw: duckdb_v2_file_list_sink_handle) -> FileListSink {
        FileListSink { raw }
    }

    pub fn as_raw(&self) -> duckdb_v2_file_list_sink_handle {
        self.raw
    }

    /// Adds an entry.
    pub fn add(&self, path: &str) -> Result<()> {
        let path = str_arg(path);
        api_call!(duckdb_v2_file_list_sink_add(self.raw, &path));
        Ok(())
    }

    /// Attaches extended information (`last_modified`, `etag`, `file_size`, `type`, ...) to the entry added last.
    pub fn set_option(&self, name: &str, value: &Value) -> Result<()> {
        let name = str_arg(name);
        api_call!(duckdb_v2_file_list_sink_set_option(self.raw, &name, value.as_raw()));
        Ok(())
    }
}

/// A logger of the engine (owned; released when dropped).
pub struct Logger {
    raw: duckdb_v2_logger_handle,
}

// SAFETY(protocol L8.I8): (T5, L8.I1) an owned `duckdb_v2_logger_handle` (`duckdb_v2.h`: "`shared_ptr<Logger>` ... An
// extension keeps it beyond the callback it got it from"; the engine's logger handle holds a `shared_ptr<Logger>`
// whose writes go through the log manager's lock); callable from any thread, the wrapper holds nothing
// else and destroys the handle once, through `&mut self`.
unsafe impl Send for Logger {}
unsafe impl Sync for Logger {}

impl Logger {
    /// # Safety
    /// (ffi) `raw` is an owned logger handle the C API returned (`duckdb_v2_file_opener_get_logger`); the wrapper
    /// destroys it on drop, so nothing else may.
    pub unsafe fn from_raw(raw: duckdb_v2_logger_handle) -> Logger {
        Logger { raw }
    }

    pub fn as_raw(&self) -> duckdb_v2_logger_handle {
        self.raw
    }

    /// `Logger::ShouldLog(type, level)`.
    pub fn should_log(&self, level: LogLevel, log_type: &str) -> Result<bool> {
        let log_type = str_arg(log_type);
        let mut should_log = false;
        api_call!(duckdb_v2_logger_should_log(self.raw, level, &log_type, &mut should_log));
        Ok(should_log)
    }

    /// `Logger::WriteLog(type, level, message)` when `ShouldLog` allows it.
    pub fn log(&self, level: LogLevel, log_type: &str, message: &str) -> Result<()> {
        let log_type = str_arg(log_type);
        let message = str_arg(message);
        api_call!(duckdb_v2_logger_log(self.raw, level, &log_type, &message));
        Ok(())
    }
}

impl Drop for Logger {
    fn drop(&mut self) {
        api_destroy!(duckdb_v2_logger_destroy(&mut self.raw));
    }
}

impl FileSystem {
    /// `FileSystem::CanonicalizePath(path)`: the absolute, normalized form of a local path.
    pub fn canonicalize(&self, path: &str) -> Result<String> {
        let path = str_arg(path);
        let mut out: Vec<String> = Vec::new();
        api_call!(duckdb_v2_file_system_path_canonicalize(
            self.as_raw(),
            &path,
            Some(collect_text),
            &mut out as *mut Vec<String> as *mut c_void
        ));
        Ok(out.pop().unwrap_or_default())
    }
}
