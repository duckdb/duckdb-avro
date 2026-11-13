//! String helpers: `duckdb_v2_str` views in and out, and the two-call "size then fill" rendering protocol
//! of `*_to_text` / `*_render` entry points.

use std::ffi::c_char;

use crate::Result;
use crate::sys::{duckdb_v2_str, idx_t};

/// A borrowed view of `s` for an `in` parameter.
pub(crate) fn str_arg(s: &str) -> duckdb_v2_str {
    duckdb_v2_str { ptr: s.as_ptr() as *const c_char, len: s.len() as idx_t }
}

/// A borrowed view of `bytes` for an `in` parameter that need not be UTF-8.
pub(crate) fn bytes_arg(bytes: &[u8]) -> duckdb_v2_str {
    duckdb_v2_str { ptr: bytes.as_ptr() as *const c_char, len: bytes.len() as idx_t }
}

/// The bytes a `duckdb_v2_str` the engine returned points at (valid only until the next API call).
///
/// # Safety
/// (ffi) T3: `s` is a view the engine returned: null / empty, or `len` readable bytes at `ptr` that stay alive (and are
/// not written) for `'a`, i.e. until the next API call.
pub(crate) unsafe fn str_bytes<'a>(s: duckdb_v2_str) -> &'a [u8] {
    if s.ptr.is_null() || s.len == 0 {
        return &[];
    }
    // SAFETY(ffi): the caller asserts (this fn is `unsafe`) that the view points at `len` live bytes.
    unsafe { std::slice::from_raw_parts(s.ptr.cast::<u8>(), s.len as usize) }
}

/// Copies a returned `duckdb_v2_str` into an owned `String` (lossily for non-UTF-8 bytes).
pub(crate) fn str_to_string(s: duckdb_v2_str) -> String {
    // SAFETY(ffi): the engine's view is live until the next API call, which happens after the copy.
    String::from_utf8_lossy(unsafe { str_bytes(s) }).into_owned()
}

/// Copies a returned `duckdb_v2_str` into owned bytes.
pub(crate) fn str_to_bytes(s: duckdb_v2_str) -> Vec<u8> {
    // SAFETY(ffi): as for `str_to_string`.
    unsafe { str_bytes(s) }.to_vec()
}

/// Renders through a `(buffer, capacity, out_needed)` entry point: the first call sizes the text, the
/// second fills it. `needed` includes the terminating NUL the C side writes.
pub(crate) fn render(mut call: impl FnMut(*mut c_char, idx_t, *mut idx_t) -> Result<()>) -> Result<String> {
    let mut needed: idx_t = 0;
    call(std::ptr::null_mut(), 0, &mut needed)?;
    let mut buffer = vec![0u8; needed as usize + 1];
    let mut written: idx_t = 0;
    call(buffer.as_mut_ptr() as *mut c_char, buffer.len() as idx_t, &mut written)?;
    let end = buffer.iter().position(|&b| b == 0).unwrap_or(buffer.len());
    buffer.truncate(end);
    Ok(String::from_utf8_lossy(&buffer).into_owned())
}

/// Renders `name` as a SQL identifier, quoted when it needs quoting (`KeywordHelper::WriteOptionallyQuoted`,
/// `duckdb_v2_identifier_render_quoted`).
pub fn quote_identifier(name: &str) -> Result<String> {
    let view = str_arg(name);
    let mut buffer = vec![0u8; name.len() * 2 + 2];
    loop {
        let mut length: idx_t = 0;
        api_call!(duckdb_v2_identifier_render_quoted(
            &view,
            buffer.as_mut_ptr() as *mut c_char,
            buffer.len() as idx_t,
            &mut length
        ));
        if (length as usize) < buffer.len() {
            buffer.truncate(length as usize);
            return Ok(String::from_utf8_lossy(&buffer).into_owned());
        }
        buffer.resize(length as usize + 1, 0);
    }
}
