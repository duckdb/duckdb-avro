//! The object cache of a database (`duckdb_v2_context_object_cache_*`): opaque objects an extension keeps across
//! queries and looks up again by type and key - e.g. the parsed metadata of a file. Entries are reference counted:
//! the cache holds one reference, every [`ObjectCacheEntry`] another, so an entry stays usable after the cache evicted
//! it (entries that report an estimated memory are charged against the database's memory limit and evicted least
//! recently used first).

use std::ffi::c_void;

use crate::Result;
use crate::context::Context;
use crate::sys::*;
use crate::text::str_arg;

unsafe extern "C" fn destroy_box<T>(data: *mut c_void) {
    if !data.is_null() {
        // SAFETY(ffi L8.I1): `data` came from `Box::into_raw` in `ObjectCacheEntry::new` with this exact `T`.
        drop(unsafe { Box::from_raw(data as *mut T) });
    }
}

/// An owned reference to an object cache entry (released when dropped).
pub struct ObjectCacheEntry {
    raw: duckdb_v2_object_cache_entry_handle,
}

// SAFETY(protocol L8.I8): (T5, L8.I1) an owned reference to an engine `ObjectCacheEntry` (`duckdb_v2.h`,
// `object_cache_put`: the object "must be safe to use from any thread, the cache is shared by every connection"):
// `ObjectCacheEntry::new` requires `T: Send + Sync` for the object it boxes, `get` lends it as `&T`, and the entry's
// reference count is the engine's atomic one.
unsafe impl Send for ObjectCacheEntry {}
unsafe impl Sync for ObjectCacheEntry {}

impl ObjectCacheEntry {
    /// Creates an entry of `object_type` around `value` (boxed; dropped with the last reference). `estimated_memory`
    /// is the rough memory the value uses in bytes - what the entry is charged for and evicted by; `None` makes the
    /// entry non-evictable.
    pub fn new<T: Send + Sync + 'static>(
        object_type: &str,
        value: T,
        estimated_memory: Option<usize>,
    ) -> Result<ObjectCacheEntry> {
        let data = Box::into_raw(Box::new(value)) as *mut c_void;
        let object_type = str_arg(object_type);
        let mut raw: duckdb_v2_object_cache_entry_handle = std::ptr::null_mut();
        let estimated_memory = estimated_memory.map(|bytes| bytes as idx_t).unwrap_or(idx_t::MAX);
        let created = (|| -> Result<()> {
            api_call!(duckdb_v2_object_cache_entry_create(
                &object_type,
                data,
                Some(destroy_box::<T>),
                estimated_memory,
                &mut raw
            ));
            Ok(())
        })();
        if let Err(error) = created {
            // SAFETY(ffi L8.I1): the entry was not created, so the box is still ours.
            drop(unsafe { Box::from_raw(data as *mut T) });
            return Err(error);
        }
        Ok(ObjectCacheEntry { raw })
    }

    /// # Safety
    /// (ffi L8.I1) `raw` must be a handle the C API returned for this type and that is still live. An owned wrapper
    /// destroys the handle on drop, so nothing else may destroy it; a borrowed (`Copy`) wrapper must not be used past
    /// the call that lent the handle.
    pub unsafe fn from_raw(raw: duckdb_v2_object_cache_entry_handle) -> ObjectCacheEntry {
        ObjectCacheEntry { raw }
    }

    pub fn as_raw(&self) -> duckdb_v2_object_cache_entry_handle {
        self.raw
    }

    /// The cached value, when the entry was created by [`ObjectCacheEntry::new`] with the same `T` (the caller
    /// knows the type from the entry's object type).
    ///
    /// # Safety
    /// (ffi L8.I1) The entry was created by [`ObjectCacheEntry::new`] with this `T`, and no `&mut` to the cached value
    /// is live (the cache shares entries between connections read-only).
    pub unsafe fn get<T>(&self) -> Result<Option<&T>> {
        let mut data: *mut c_void = std::ptr::null_mut();
        api_call!(duckdb_v2_object_cache_entry_get_data(self.raw, &mut data));
        // SAFETY(ffi L8.I1): the caller asserts (this fn is `unsafe`) that the entry was created by
        // `ObjectCacheEntry::new` with this `T`; the entry keeps the data alive.
        Ok(if data.is_null() { None } else { Some(unsafe { &*(data as *const T) }) })
    }
}

impl Drop for ObjectCacheEntry {
    fn drop(&mut self) {
        api_destroy!(duckdb_v2_object_cache_entry_destroy(&mut self.raw));
    }
}

impl Context {
    /// Whether the loading engine has an object cache for extensions (an upstream engine has not).
    pub fn has_object_cache() -> bool {
        crate::api::api().duckdb_v2_context_object_cache_get.is_some()
    }

    /// `ObjectCache::GetWithTypePrefix<T>(key)`: the entry of `object_type` under `key`, if cached.
    pub fn object_cache_get(&self, object_type: &str, key: &str) -> Result<Option<ObjectCacheEntry>> {
        let object_type = str_arg(object_type);
        let key = str_arg(key);
        let mut raw: duckdb_v2_object_cache_entry_handle = std::ptr::null_mut();
        api_call!(duckdb_v2_context_object_cache_get(self.as_raw(), &object_type, &key, &mut raw));
        Ok(if raw.is_null() { None } else { Some(ObjectCacheEntry { raw }) })
    }

    /// `ObjectCache::PutWithTypePrefix<T>(key, entry)`: caches the entry under `key` (namespaced by its type).
    pub fn object_cache_put(&self, key: &str, entry: &ObjectCacheEntry) -> Result<()> {
        let key = str_arg(key);
        api_call!(duckdb_v2_context_object_cache_put(self.as_raw(), &key, entry.raw));
        Ok(())
    }

    /// `ObjectCache::DeleteWithTypePrefix<T>(key)`.
    pub fn object_cache_delete(&self, object_type: &str, key: &str) -> Result<()> {
        let object_type = str_arg(object_type);
        let key = str_arg(key);
        api_call!(duckdb_v2_context_object_cache_delete(self.as_raw(), &object_type, &key));
        Ok(())
    }
}
