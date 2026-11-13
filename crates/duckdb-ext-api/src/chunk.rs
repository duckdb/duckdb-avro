//! Data chunks (`duckdb_v2_data_chunk_handle`): owned when created or fetched from a result, borrowed
//! when the engine hands one to a callback.

use crate::Result;
use crate::context::Factory;
use crate::sys::*;
use crate::types::LogicalType;
use crate::vector::Vector;

/// A data chunk: a row count plus one vector per column.
pub struct DataChunk {
    raw: duckdb_v2_data_chunk_handle,
    owned: bool,
}

// SAFETY(protocol L8.I8): (T5, L8.I1) an owned or borrowed `duckdb_v2_data_chunk_handle` (`duckdb_v2.h`: a chunk "may
// be owned or borrowed, depending on the context"); the engine's `DataChunk` has no thread affinity, so the handle
// moves to another thread as a whole (a borrowed chunk only within the callback that lent it). Not `Sync`: the chunk
// entries mutate through `&self` (`set_size`, `clear`, the vectors' data), so a chunk is used by one thread at a time.
unsafe impl Send for DataChunk {}

impl DataChunk {
    /// Takes ownership of a chunk the API returned.
    /// # Safety
    /// (ffi L8.I1) `raw` must be a handle the C API returned for this type and that is still live. An owned wrapper
    /// destroys the handle on drop, so nothing else may destroy it; a borrowed (`Copy`) wrapper must not be used past
    /// the call that lent the handle.
    #[inline]
    pub unsafe fn from_raw(raw: duckdb_v2_data_chunk_handle) -> DataChunk {
        DataChunk { raw, owned: true }
    }

    /// A view of a chunk the engine owns (a callback's output chunk).
    /// # Safety
    /// (ffi L8.I1) `raw` must be a handle the C API returned for this type and that is still live. An owned wrapper
    /// destroys the handle on drop, so nothing else may destroy it; a borrowed (`Copy`) wrapper must not be used past
    /// the call that lent the handle.
    #[inline]
    pub unsafe fn borrowed(raw: duckdb_v2_data_chunk_handle) -> DataChunk {
        DataChunk { raw, owned: false }
    }

    #[inline]
    pub fn as_raw(&self) -> duckdb_v2_data_chunk_handle {
        self.raw
    }

    /// An empty chunk with one FLAT vector per type, allocated through the factory's database.
    pub fn new(factory: Factory, types: &[&LogicalType]) -> Result<DataChunk> {
        let raws: Vec<duckdb_v2_logical_type_handle> = types.iter().map(|t| t.as_raw()).collect();
        let mut raw = std::ptr::null_mut();
        api_call!(duckdb_v2_data_chunk_create(factory.as_raw(), raws.as_ptr(), raws.len() as idx_t, &mut raw));
        Ok(DataChunk { raw, owned: true })
    }

    /// A deep copy.
    pub fn copy(&self, factory: Factory) -> Result<DataChunk> {
        let mut raw: duckdb_v2_data_chunk_handle = std::ptr::null_mut();
        api_call!(duckdb_v2_data_chunk_copy(factory.as_raw(), self.raw, &mut raw));
        Ok(DataChunk { raw, owned: true })
    }

    /// The number of rows.
    #[inline]
    pub fn size(&self) -> Result<usize> {
        let mut size: idx_t = 0;
        api_call!(duckdb_v2_data_chunk_get_size(self.raw, &mut size));
        Ok(size as usize)
    }

    /// Sets the number of rows (through the vectors: every column's size is the chunk's).
    pub fn set_size(&self, size: usize) -> Result<()> {
        for i in 0..self.column_count()? {
            self.vector(i)?.set_size(size)?;
        }
        Ok(())
    }

    /// The row capacity of the chunk's vectors.
    #[inline]
    pub fn capacity(&self) -> Result<usize> {
        let mut capacity: idx_t = 0;
        api_call!(duckdb_v2_data_chunk_get_capacity(self.raw, &mut capacity));
        Ok(capacity as usize)
    }

    #[inline]
    pub fn column_count(&self) -> Result<usize> {
        let mut count: idx_t = 0;
        api_call!(duckdb_v2_data_chunk_get_vector_count(self.raw, &mut count));
        Ok(count as usize)
    }

    #[inline]
    pub fn vector(&self, column: usize) -> Result<Vector> {
        Ok(api_call_out!(duckdb_v2_data_chunk_get_vector(self.raw, column as idx_t) -> Vector::from_raw))
    }

    /// Resets the chunk to zero rows.
    #[inline]
    pub fn clear(&self) -> Result<()> {
        api_call!(duckdb_v2_data_chunk_clear(self.raw));
        Ok(())
    }
}

impl Drop for DataChunk {
    fn drop(&mut self) {
        if self.owned {
            api_destroy!(duckdb_v2_data_chunk_destroy(&mut self.raw));
        }
    }
}
