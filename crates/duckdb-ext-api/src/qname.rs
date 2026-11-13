//! Qualified names (`duckdb_v2_qname_handle`, owned): `catalog.schema.name`.

use std::fmt;

use crate::Result;
use crate::sys::*;
use crate::text::{render, str_arg, str_to_string};

/// An owned qualified name.
pub struct QualifiedName {
    raw: duckdb_v2_qname_handle,
}

// SAFETY(protocol L8.I8): (T5, L8.I1) an owned `duckdb_v2_qname_handle` (`duckdb_v2.h`: constructed by `qname_parse` /
// `qname_create`, then read with `get_part_count` / `get_part`, rendered and compared); every entry the wrapper calls
// after construction reads the name, so a shared `&QualifiedName` is a shared read of a plain value, and the owned name
// moves between threads as a whole.
unsafe impl Send for QualifiedName {}
unsafe impl Sync for QualifiedName {}

impl QualifiedName {
    /// # Safety
    /// (ffi L8.I1) `raw` must be a handle the C API returned for this type and that is still live. An owned wrapper
    /// destroys the handle on drop, so nothing else may destroy it; a borrowed (`Copy`) wrapper must not be used past
    /// the call that lent the handle.
    pub unsafe fn from_raw(raw: duckdb_v2_qname_handle) -> QualifiedName {
        QualifiedName { raw }
    }

    pub fn as_raw(&self) -> duckdb_v2_qname_handle {
        self.raw
    }

    /// Parses `"schema.name"` / `"\"quoted\".name"` text.
    pub fn parse(text: &str) -> Result<QualifiedName> {
        let text = str_arg(text);
        let mut raw: duckdb_v2_qname_handle = std::ptr::null_mut();
        api_call!(duckdb_v2_qname_parse(&text, &mut raw));
        Ok(QualifiedName { raw })
    }

    /// Builds a name from its parts (1 to 3: `[catalog,] [schema,] name`).
    pub fn new(parts: &[&str]) -> Result<QualifiedName> {
        let parts: Vec<duckdb_v2_identifier_t> = parts.iter().map(|p| str_arg(p)).collect();
        let mut raw: duckdb_v2_qname_handle = std::ptr::null_mut();
        api_call!(duckdb_v2_qname_create(parts.as_ptr(), parts.len() as idx_t, &mut raw));
        Ok(QualifiedName { raw })
    }

    pub fn part_count(&self) -> Result<usize> {
        let mut count: idx_t = 0;
        api_call!(duckdb_v2_qname_get_part_count(self.raw, &mut count));
        Ok(count as usize)
    }

    pub fn part(&self, index: usize) -> Result<String> {
        let mut part = duckdb_v2_str::EMPTY;
        api_call!(duckdb_v2_qname_get_part(self.raw, index as idx_t, &mut part));
        Ok(str_to_string(part))
    }

    pub fn parts(&self) -> Result<Vec<String>> {
        (0..self.part_count()?).map(|i| self.part(i)).collect()
    }

    /// The quoted SQL rendering.
    pub fn render(&self) -> Result<String> {
        render(|buffer, capacity, needed| {
            api_call!(duckdb_v2_qname_render(self.raw, buffer, capacity, needed));
            Ok(())
        })
    }

    pub fn equals(&self, other: &QualifiedName) -> Result<bool> {
        let mut equal = false;
        api_call!(duckdb_v2_qname_equals(self.raw, other.raw, &mut equal));
        Ok(equal)
    }
}

impl Drop for QualifiedName {
    fn drop(&mut self) {
        api_destroy!(duckdb_v2_qname_destroy(&mut self.raw));
    }
}

impl fmt::Debug for QualifiedName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.render() {
            Ok(text) => f.write_str(&text),
            Err(error) => write!(f, "<qname: {error}>"),
        }
    }
}
