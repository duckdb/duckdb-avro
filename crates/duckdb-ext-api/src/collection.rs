//! Column data collections (`duckdb_v2_column_data_collection_handle`): the rows a COPY TO batch receives, scanned
//! chunk by chunk through a shared scan state (the position) and one worker scan state per scanning thread (the
//! pins of the chunk it last produced).

use std::sync::Arc;

use crate::Result;
use crate::chunk::DataChunk;
use crate::context::Factory;
use crate::sys::*;
use crate::types::LogicalType;

/// The owned handle, shared by the collection and the scan states made over it, so that the states are destroyed
/// before it however they are dropped.
struct CollectionHandle {
    raw: duckdb_v2_column_data_collection_handle,
}

// SAFETY(protocol L8.I8): (T5, L8.I1) an owned engine `ColumnDataCollection` (`duckdb_v2.h`: "shall not be appended to
// concurrently, but it can be scanned concurrently by multiple threads using a shared scan state and per-thread worker
// scan states"). The appending entries (`append`, `clear`, `append_state`: the engine's `&mut self` methods) are
// reached through `&mut ColumnDataCollection`, the one owner of mutation - the `Arc` clones inside the scan and append
// states only keep the handle alive, and `ColumnDataCollection` is not `Clone`; the scanning entries (`scan` under the
// shared state's mutex, the state constructors, `row_count`) are the engine's `&self` entries, shared by the scanning
// threads. What the type does not enforce is the header's "shall not be scanned while it is being appended to" (a
// worker state on another thread while `append` runs): the embedder's contract, as in the C++.
unsafe impl Send for CollectionHandle {}
unsafe impl Sync for CollectionHandle {}

impl Drop for CollectionHandle {
    fn drop(&mut self) {
        api_destroy!(duckdb_v2_column_data_collection_destroy(&mut self.raw));
    }
}

/// An owned column data collection ([`crate::copy::CopyToBatchInfo::take_input`]): appended to through `&mut self`,
/// scanned through `&self` by any number of threads with a [`SharedScanState`] and their [`WorkerScanState`]s (which
/// keep the collection alive).
pub struct ColumnDataCollection {
    handle: Arc<CollectionHandle>,
}

impl ColumnDataCollection {
    /// Takes ownership of a collection handle the API returned.
    /// # Safety
    /// (ffi L8.I1) `raw` must be a handle the C API returned for this type and that is still live. An owned wrapper
    /// destroys the handle on drop, so nothing else may destroy it; a borrowed (`Copy`) wrapper must not be used past
    /// the call that lent the handle.
    pub unsafe fn from_raw(raw: duckdb_v2_column_data_collection_handle) -> ColumnDataCollection {
        ColumnDataCollection { handle: Arc::new(CollectionHandle { raw }) }
    }

    pub fn as_raw(&self) -> duckdb_v2_column_data_collection_handle {
        self.handle.raw
    }

    /// An empty collection of `types`, allocated through the factory's database
    /// (`duckdb_v2_column_data_collection_create`).
    pub fn new(factory: Factory, types: &[&LogicalType]) -> Result<ColumnDataCollection> {
        let raws: Vec<duckdb_v2_logical_type_handle> = types.iter().map(|t| t.as_raw()).collect();
        let mut raw: duckdb_v2_column_data_collection_handle = std::ptr::null_mut();
        api_call!(duckdb_v2_column_data_collection_create(
            factory.as_raw(),
            raws.as_ptr(),
            raws.len() as idx_t,
            &mut raw
        ));
        Ok(ColumnDataCollection { handle: Arc::new(CollectionHandle { raw }) })
    }

    /// `ColumnDataCollection::Count()`.
    pub fn row_count(&self) -> Result<usize> {
        let mut count: idx_t = 0;
        api_call!(duckdb_v2_column_data_collection_row_count(self.handle.raw, &mut count));
        Ok(count as usize)
    }

    /// Removes every row; the append states made before are stale afterwards (make a new one). An appending entry
    /// (the engine's `ResetForReuse`), so `&mut self`.
    pub fn clear(&mut self) -> Result<()> {
        api_call!(duckdb_v2_column_data_collection_clear(self.handle.raw));
        Ok(())
    }

    /// `ColumnDataAppendState`: the position appends go to (the engine's `InitializeAppend`, an appending entry).
    pub fn append_state(&mut self) -> Result<AppendState> {
        let mut raw: duckdb_v2_column_data_collection_append_state_handle = std::ptr::null_mut();
        api_call!(duckdb_v2_column_data_collection_append_state_create(self.handle.raw, &mut raw));
        Ok(AppendState { raw, _collection: self.handle.clone() })
    }

    /// `ColumnDataCollection::Append(state, chunk)`: copies the chunk's rows into the collection. Appends are
    /// exclusive (`duckdb_v2.h`: a collection "shall not be appended to concurrently"), hence `&mut self` and the
    /// `&mut` append state the engine advances.
    pub fn append(&mut self, state: &mut AppendState, chunk: &DataChunk) -> Result<()> {
        api_call!(duckdb_v2_column_data_collection_append(self.handle.raw, state.raw, chunk.as_raw()));
        Ok(())
    }

    /// A shared scan state: the position of a scan over the whole collection, shared by its worker states.
    pub fn shared_scan_state(&self) -> Result<SharedScanState> {
        let mut raw: duckdb_v2_column_data_collection_shared_scan_state_handle = std::ptr::null_mut();
        api_call!(duckdb_v2_column_data_collection_shared_scan_state_create(self.handle.raw, &mut raw));
        Ok(SharedScanState { raw, _collection: self.handle.clone() })
    }

    /// A worker scan state: pins the collection memory of the chunk it last produced (released when it scans the
    /// next one or is dropped), so a chunk's vectors stay valid as long as the worker state that produced it.
    pub fn worker_scan_state(&self) -> Result<WorkerScanState> {
        let mut raw: duckdb_v2_column_data_collection_worker_scan_state_handle = std::ptr::null_mut();
        api_call!(duckdb_v2_column_data_collection_worker_scan_state_create(self.handle.raw, &mut raw));
        Ok(WorkerScanState { raw, _collection: self.handle.clone() })
    }

    /// Scans the next chunk into `chunk` (created with the collection's types): `Ok(false)` once the collection is
    /// exhausted. The chunk's vectors reference collection memory pinned by `worker`: read them before the worker
    /// scans again or is dropped. The shared state is shared (the engine locks it per call), the worker state is
    /// this thread's and advances (`&mut`).
    pub fn scan(&self, shared: &SharedScanState, worker: &mut WorkerScanState, chunk: &DataChunk) -> Result<bool> {
        let mut produced = false;
        api_call!(duckdb_v2_column_data_collection_scan(
            self.handle.raw,
            shared.raw,
            worker.raw,
            chunk.as_raw(),
            &mut produced
        ));
        Ok(produced)
    }
}

/// The shared position of a scan ([`ColumnDataCollection::shared_scan_state`]); keeps its collection alive.
pub struct SharedScanState {
    raw: duckdb_v2_column_data_collection_shared_scan_state_handle,
    _collection: Arc<CollectionHandle>,
}

// SAFETY(protocol L8.I8): (T5, L8.I1) an owned engine `ColumnDataParallelScanState` (`duckdb_v2.h`: "shared state
// across multiple threads scanning a column data collection"); the engine keeps its position behind a mutex
// (`ColumnDataParallelScanState::lock`), which every `scan_parallel` locks, so the handle is shared by the
// scanning threads by design; the wrapper holds only the `Arc` that keeps the collection alive.
unsafe impl Send for SharedScanState {}
unsafe impl Sync for SharedScanState {}

impl SharedScanState {
    pub fn as_raw(&self) -> duckdb_v2_column_data_collection_shared_scan_state_handle {
        self.raw
    }
}

impl Drop for SharedScanState {
    fn drop(&mut self) {
        api_destroy!(duckdb_v2_column_data_collection_shared_scan_state_destroy(&mut self.raw));
    }
}

/// One scanning thread's state ([`ColumnDataCollection::worker_scan_state`]); keeps its collection alive.
pub struct WorkerScanState {
    raw: duckdb_v2_column_data_collection_worker_scan_state_handle,
    _collection: Arc<CollectionHandle>,
}

// SAFETY(protocol L8.I8): (T5, L8.I1) an owned engine `ColumnDataLocalScanState` (`duckdb_v2.h`: "state that needs to
// be maintained on a per-thread basis"); the engine's `scan_parallel` takes it `&mut`, mirrored by
// `ColumnDataCollection::scan`'s `&mut WorkerScanState`, so a state is used by one thread at a time and moves between
// threads with its owner (`Send`, not `Sync`); the wrapper holds only the `Arc` that keeps the collection alive.
unsafe impl Send for WorkerScanState {}

impl WorkerScanState {
    pub fn as_raw(&self) -> duckdb_v2_column_data_collection_worker_scan_state_handle {
        self.raw
    }
}

impl Drop for WorkerScanState {
    fn drop(&mut self) {
        api_destroy!(duckdb_v2_column_data_collection_worker_scan_state_destroy(&mut self.raw));
    }
}

/// The position of appends into a collection ([`ColumnDataCollection::append_state`]); keeps its collection alive.
pub struct AppendState {
    raw: duckdb_v2_column_data_collection_append_state_handle,
    _collection: Arc<CollectionHandle>,
}

// SAFETY(protocol L8.I8): (T5, L8.I1) an owned engine `ColumnDataAppendState` (`duckdb_v2.h`: "state that needs to be
// maintained across calls while appending"); the engine's `append_with_state` takes it `&mut`, mirrored by
// `ColumnDataCollection::append`, so a state is used by one thread at a time and moves between threads with its owner
// (`Send`, not `Sync`); the wrapper holds only the `Arc` that keeps the collection alive.
unsafe impl Send for AppendState {}

impl AppendState {
    pub fn as_raw(&self) -> duckdb_v2_column_data_collection_append_state_handle {
        self.raw
    }
}

impl Drop for AppendState {
    fn drop(&mut self) {
        api_destroy!(duckdb_v2_column_data_collection_append_state_destroy(&mut self.raw));
    }
}
