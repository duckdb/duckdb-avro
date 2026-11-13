//! The engine's file system: open, read and write files through the same layers the engine uses
//! (`file_search_path`, `enable_external_access`, remote file systems registered by extensions).

use std::ffi::c_void;

use crate::Result;
use crate::sys::*;
use crate::text::{str_arg, str_to_string};
use crate::value::Value;

/// `DUCKDB_V2_FILE_FLAG` (`READ`, `WRITE`, `CREATE`, `CREATE_NEW`, `APPEND`, ...).
pub type FileFlag = DUCKDB_V2_FILE_FLAG;

/// `DUCKDB_V2_REQUEST_SIZING` (`BY_CACHE`, `BY_READER`): how the external file cache sizes a file's reads.
pub type RequestSizing = DUCKDB_V2_REQUEST_SIZING;

/// A file system (borrowed from an instance, connection or context).
#[derive(Clone, Copy)]
pub struct FileSystem {
    raw: duckdb_v2_file_system_handle,
}

/// The sink of the text-listing entries (`glob`, directory listings): appends each entry the engine reports to the
/// `Vec<String>` registered as `data`.
///
/// # Safety
/// T4 / T3: called by the engine with `data` = the `*mut Vec<String>` this crate passed to the listing call, for the
/// duration of that call, and `text` null or a `duckdb_v2_str` view valid for the duration of the callback.
pub(crate) unsafe extern "C" fn collect_text(
    text: *const duckdb_v2_str,
    data: *mut c_void,
    _err: *mut duckdb_v2_error_info_handle,
) {
    if text.is_null() || data.is_null() {
        return;
    }
    // SAFETY(ffi): `data` is the `Vec<String>` the caller passed; `text` is a live view.
    let out = unsafe { &mut *(data as *mut Vec<String>) };
    // SAFETY(ffi): the glob entry calls back with a live path view for the duration of the call.
    out.push(str_to_string(unsafe { *text }));
}

impl FileSystem {
    /// # Safety
    /// (ffi) `raw` must be a handle the C API returned for this type and that is still live. An owned wrapper destroys
    /// the handle on drop, so nothing else may destroy it; a borrowed (`Copy`) wrapper must not be used past the
    /// call that lent the handle.
    pub unsafe fn from_raw(raw: duckdb_v2_file_system_handle) -> FileSystem {
        FileSystem { raw }
    }

    pub fn as_raw(&self) -> duckdb_v2_file_system_handle {
        self.raw
    }

    pub fn open_options(&self) -> Result<FileOpenOptions> {
        let mut raw: duckdb_v2_file_open_options_handle = std::ptr::null_mut();
        api_call!(duckdb_v2_file_open_options_create(self.raw, &mut raw));
        Ok(FileOpenOptions { raw })
    }

    /// Opens `path` with `options`.
    pub fn open(&self, path: &str, options: &FileOpenOptions) -> Result<File> {
        let path = str_arg(path);
        let mut raw: duckdb_v2_file_handle = std::ptr::null_mut();
        api_call!(duckdb_v2_file_system_open(self.raw, &path, options.raw, &mut raw));
        Ok(File { raw })
    }

    /// Opens `path` for reading.
    pub fn open_read(&self, path: &str) -> Result<File> {
        let options = self.open_options()?;
        options.set_flag(DUCKDB_V2_FILE_FLAG_READ)?;
        self.open(path, &options)
    }

    pub fn exists(&self, path: &str) -> Result<bool> {
        let path = str_arg(path);
        let mut exists = false;
        api_call!(duckdb_v2_file_system_path_exists(self.raw, &path, &mut exists));
        Ok(exists)
    }

    /// Whether `path` names a pipe or character device (e.g. `/dev/stdout`).
    pub fn is_pipe(&self, path: &str) -> Result<bool> {
        let path = str_arg(path);
        let mut is_pipe = false;
        api_call!(duckdb_v2_file_system_path_is_pipe(self.raw, &path, &mut is_pipe));
        Ok(is_pipe)
    }

    pub fn is_remote(&self, path: &str) -> Result<bool> {
        let path = str_arg(path);
        let mut remote = false;
        api_call!(duckdb_v2_file_system_path_is_remote(self.raw, &path, &mut remote));
        Ok(remote)
    }

    /// Expands `~` and the like.
    pub fn expand(&self, path: &str) -> Result<String> {
        let path = str_arg(path);
        let mut out: Vec<String> = Vec::new();
        api_call!(duckdb_v2_file_system_path_expand(
            self.raw,
            &path,
            Some(collect_text),
            &mut out as *mut Vec<String> as *mut c_void
        ));
        Ok(out.pop().unwrap_or_default())
    }

    /// The files matching a glob pattern.
    pub fn glob(&self, pattern: &str) -> Result<Vec<String>> {
        let pattern = str_arg(pattern);
        let mut out: Vec<String> = Vec::new();
        api_call!(duckdb_v2_file_system_path_glob(
            self.raw,
            &pattern,
            Some(collect_text),
            &mut out as *mut Vec<String> as *mut c_void
        ));
        Ok(out)
    }

    pub fn move_file(&self, source: &str, target: &str) -> Result<()> {
        let source = str_arg(source);
        let target = str_arg(target);
        api_call!(duckdb_v2_file_system_path_move(self.raw, &source, &target));
        Ok(())
    }

    /// Removes a file; `false` when there was none.
    pub fn remove(&self, path: &str) -> Result<bool> {
        let path = str_arg(path);
        let mut removed = false;
        api_call!(duckdb_v2_file_system_path_remove(self.raw, &path, &mut removed));
        Ok(removed)
    }
}

/// How to open a file (owned).
pub struct FileOpenOptions {
    raw: duckdb_v2_file_open_options_handle,
}

impl FileOpenOptions {
    pub fn set_flag(&self, flag: FileFlag) -> Result<&Self> {
        api_call!(duckdb_v2_file_open_options_set_flag(self.raw, flag));
        Ok(self)
    }

    /// `FileOpenFlags::SetRequestSizing`: how the external file cache sizes the reads of the file (`BY_CACHE` unless
    /// set: a read is widened to the cache's block grid; `BY_READER`: the bytes the reader asked for).
    pub fn set_request_sizing(&self, sizing: RequestSizing) -> Result<&Self> {
        api_call!(duckdb_v2_file_open_options_set_request_sizing(self.raw, sizing));
        Ok(self)
    }

    /// An extended option (e.g. an encryption key, a file size hint).
    pub fn set_value(&self, key: &str, value: &Value) -> Result<&Self> {
        let key = str_arg(key);
        api_call!(duckdb_v2_file_open_options_set_value(self.raw, &key, value.as_raw()));
        Ok(self)
    }
}

impl Drop for FileOpenOptions {
    fn drop(&mut self) {
        api_destroy!(duckdb_v2_file_open_options_destroy(&mut self.raw));
    }
}

/// An open file (owned; closed when dropped).
pub struct File {
    raw: duckdb_v2_file_handle,
}

// SAFETY(protocol L8.I8): (T5, L8.I1) an owned `duckdb_v2_file_handle` (`duckdb_v2.h`: "Read, write, seek and sync
// through the `file_*` functions"); the engine's file handle has no thread affinity, so it moves to another thread as a
// whole. Not `Sync`: the sequential entries (`read`, `write`, `seek`) move the file position through `&self`, so a file
// is used by one thread at a time; the positional `read_at` / `write_at` are what the header allows from several
// threads under `FILE_FLAG_PARALLEL_ACCESS`, and a sharer states that itself (parquet's `ParquetFile: Sync`).
unsafe impl Send for File {}

impl File {
    pub fn as_raw(&self) -> duckdb_v2_file_handle {
        self.raw
    }

    /// Reads at the current position; returns the number of bytes read (0 at the end).
    pub fn read(&self, buffer: &mut [u8]) -> Result<usize> {
        let mut read: idx_t = 0;
        api_call!(duckdb_v2_file_read(self.raw, buffer.as_mut_ptr() as *mut c_void, buffer.len() as idx_t, &mut read));
        Ok(read as usize)
    }

    /// Reads exactly `buffer.len()` bytes at `offset`.
    pub fn read_at(&self, buffer: &mut [u8], offset: u64) -> Result<()> {
        api_call!(duckdb_v2_file_read_at(self.raw, buffer.as_mut_ptr() as *mut c_void, buffer.len() as idx_t, offset));
        Ok(())
    }

    /// Writes at the current position; returns the number of bytes written.
    pub fn write(&self, buffer: &[u8]) -> Result<usize> {
        let mut written: idx_t = 0;
        api_call!(duckdb_v2_file_write(
            self.raw,
            buffer.as_ptr() as *const c_void,
            buffer.len() as idx_t,
            &mut written
        ));
        Ok(written as usize)
    }

    pub fn write_at(&self, buffer: &[u8], offset: u64) -> Result<()> {
        api_call!(duckdb_v2_file_write_at(self.raw, buffer.as_ptr() as *const c_void, buffer.len() as idx_t, offset));
        Ok(())
    }

    pub fn seek(&self, offset: u64) -> Result<()> {
        api_call!(duckdb_v2_file_seek(self.raw, offset));
        Ok(())
    }

    pub fn tell(&self) -> Result<u64> {
        let mut position: idx_t = 0;
        api_call!(duckdb_v2_file_tell(self.raw, &mut position));
        Ok(position)
    }

    pub fn size(&self) -> Result<u64> {
        let mut size: idx_t = 0;
        api_call!(duckdb_v2_file_size(self.raw, &mut size));
        Ok(size)
    }

    /// `FileHandle::GetLastModifiedTime()`: microseconds since the Unix epoch, as the file system reports it.
    pub fn last_modified_time(&self) -> Result<i64> {
        let mut time: i64 = 0;
        api_call!(duckdb_v2_file_get_last_modified_time(self.raw, &mut time));
        Ok(time)
    }

    /// `FileHandle::GetVersionTag()`: the opaque bytes that change whenever the file does (empty when the file
    /// system provides none).
    pub fn version_tag(&self) -> Result<Vec<u8>> {
        let mut length: idx_t = 0;
        api_call!(duckdb_v2_file_get_version_tag(self.raw, std::ptr::null_mut(), 0, &mut length));
        let mut tag = vec![0u8; length as usize];
        if length > 0 {
            api_call!(duckdb_v2_file_get_version_tag(self.raw, tag.as_mut_ptr(), length, &mut length));
            tag.truncate(length as usize);
        }
        Ok(tag)
    }

    /// `FileHandle::TryGetNetworkThroughput()`: the `(latency_seconds, bandwidth_bytes_per_second)` measured on the
    /// file's requests, when the file system measures them (`None` on an older host without the entry).
    pub fn try_get_network_throughput(&self) -> Result<Option<(f64, f64)>> {
        if crate::api().duckdb_v2_file_try_get_network_throughput.is_none() {
            return Ok(None);
        }
        let (mut latency, mut bandwidth, mut has_estimate) = (0.0f64, 0.0f64, false);
        api_call!(duckdb_v2_file_try_get_network_throughput(self.raw, &mut latency, &mut bandwidth, &mut has_estimate));
        Ok(has_estimate.then_some((latency, bandwidth)))
    }

    pub fn sync(&self) -> Result<()> {
        api_call!(duckdb_v2_file_sync(self.raw));
        Ok(())
    }

    pub fn close(&self) -> Result<()> {
        api_call!(duckdb_v2_file_close(self.raw));
        Ok(())
    }

    /// The whole file as bytes.
    pub fn read_to_end(&self) -> Result<Vec<u8>> {
        let size = self.size()? as usize;
        let mut buffer = vec![0u8; size];
        if size > 0 {
            self.read_at(&mut buffer, 0)?;
        }
        Ok(buffer)
    }
}

impl Drop for File {
    fn drop(&mut self) {
        api_destroy!(duckdb_v2_file_destroy(&mut self.raw));
    }
}

/// The engine's `AsyncFileWriter` over a file of a context's file system (owned; an unclosed writer abandons its
/// file when dropped): writes are registered at the next logical offset and drained on the engine's async task pool,
/// coalesced for remote targets. Calls on one writer must be serialized by the caller.
pub struct AsyncFileWriter {
    raw: duckdb_v2_async_file_writer_handle,
}

// SAFETY(protocol L8.I8): (T5, L8.I1) an owned `duckdb_v2_async_file_writer_handle` (`duckdb_v2.h`: "Calls on one
// writer must be serialized by the caller"); the engine's writer has no thread affinity (its drain tasks hold their own
// references), so it moves to another thread as a whole. Not `Sync`: the entries move the logical stream position
// through `&self`, so a writer is used by one thread at a time (parquet's writer locks its file state around it).
unsafe impl Send for AsyncFileWriter {}

impl AsyncFileWriter {
    /// `AsyncFileWriter(context, fs, path, flags)`: opens `path` for writing through the context's file system;
    /// without `options` with the writer's default flags (write + create).
    pub fn open(
        context: &crate::context::Context,
        path: &str,
        options: Option<&FileOpenOptions>,
    ) -> Result<AsyncFileWriter> {
        let path = str_arg(path);
        let options = options.map_or(std::ptr::null_mut(), |options| options.raw);
        let mut raw: duckdb_v2_async_file_writer_handle = std::ptr::null_mut();
        api_call!(duckdb_v2_async_file_writer_open(context.as_raw(), &path, options, &mut raw));
        Ok(AsyncFileWriter { raw })
    }

    pub fn as_raw(&self) -> duckdb_v2_async_file_writer_handle {
        self.raw
    }

    /// `WriteData(buffer, size)`: registers a copy of the bytes at the next logical offset.
    pub fn write(&self, buffer: &[u8]) -> Result<()> {
        api_call!(duckdb_v2_async_file_writer_write(self.raw, buffer.as_ptr() as *const c_void, buffer.len() as idx_t));
        Ok(())
    }

    /// `WriteData(unique_ptr<AsyncWriteBuffer>)`: hands the buffer over at the next logical offset; the engine's
    /// drain task writes it in place and frees it afterwards (no copy; the parquet writer's pages take this path).
    pub fn write_owned(&self, buffer: Vec<u8>) -> Result<()> {
        let size = buffer.len() as idx_t;
        let boxed = Box::new(buffer);
        let ptr = boxed.as_ptr() as *const c_void;
        // the engine owns the box from the call on, failures included: `destroy_owned_buffer` frees it
        let data = Box::into_raw(boxed) as *mut c_void;
        api_call!(duckdb_v2_async_file_writer_write_owned(self.raw, ptr, size, data, Some(destroy_owned_buffer)));
        Ok(())
    }

    /// `StartBatch()`: delays the draining of the writes registered until the batch is finished or left.
    pub fn begin_batch(&self) -> Result<()> {
        api_call!(duckdb_v2_async_file_writer_begin_batch(self.raw));
        Ok(())
    }

    /// `BatchGuard::Finish()`: leaves the batch and applies the writer's post-batch scheduling / backpressure.
    pub fn finish_batch(&self) -> Result<()> {
        api_call!(duckdb_v2_async_file_writer_finish_batch(self.raw));
        Ok(())
    }

    /// The guard's drop path: leaves the batch without scheduling, blocking or failing.
    pub fn leave_batch(&self) {
        if let Some(f) = crate::api::api().duckdb_v2_async_file_writer_leave_batch {
            let mut err: duckdb_v2_error_info_handle = std::ptr::null_mut();
            // SAFETY(ffi): the table entry is the engine's `duckdb_v2_async_file_writer_leave_batch`; `self.raw` is the
            // live writer handle and `err` the error slot it may fill (released below).
            let code = unsafe { f(self.raw, &mut err) };
            let _ = crate::error::check(code, err);
        }
    }

    /// `GetTotalWritten()`: the logical number of bytes written, including the writes still pending.
    pub fn total_written(&self) -> Result<u64> {
        let mut total: idx_t = 0;
        api_call!(duckdb_v2_async_file_writer_total_written(self.raw, &mut total));
        Ok(total)
    }

    /// `Flush()`: waits until every registered write reached the file.
    pub fn flush(&self) -> Result<()> {
        api_call!(duckdb_v2_async_file_writer_flush(self.raw));
        Ok(())
    }

    /// `Close()`: waits for all writes, then closes the file.
    pub fn close(&self) -> Result<()> {
        api_call!(duckdb_v2_async_file_writer_close(self.raw));
        Ok(())
    }

    /// `AbortWrite()`: discards the pending writes and abandons the incomplete file.
    pub fn abort(&self) -> Result<()> {
        api_call!(duckdb_v2_async_file_writer_abort(self.raw));
        Ok(())
    }
}

impl Drop for AsyncFileWriter {
    fn drop(&mut self) {
        api_destroy!(duckdb_v2_async_file_writer_destroy(&mut self.raw));
    }
}

/// The `destroy` of [`AsyncFileWriter::write_owned`]: frees the boxed buffer once the engine is done with it.
unsafe extern "C" fn destroy_owned_buffer(data: *mut c_void) {
    if !data.is_null() {
        // SAFETY(ffi L8.I1): `data` is the `Box<Vec<u8>>` `write_owned` leaked; the engine calls `destroy` once.
        drop(unsafe { Box::from_raw(data as *mut Vec<u8>) });
    }
}
