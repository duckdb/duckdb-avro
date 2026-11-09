//! Vectors (`duckdb_v2_vector_handle`, borrowed from a chunk or a parent vector).

use std::ffi::c_void;

use crate::cast::CastMode;
use crate::context::Factory;
use crate::sys::*;
use crate::types::{LogicalType, TypeId};
use crate::value::Value;
use crate::{Error, Result};

/// `DUCKDB_V2_VECTOR_TYPE` (`DUCKDB_V2_VECTOR_TYPE_FLAT` / `CONSTANT` / `DICTIONARY` / `OTHER`).
pub type VectorType = DUCKDB_V2_VECTOR_TYPE;

/// A borrowed vector: valid as long as the chunk (or callback) that handed it out.
#[derive(Clone, Copy)]
pub struct Vector {
    raw: duckdb_v2_vector_handle,
}

impl Vector {
    /// # Safety
    /// (ffi L8.I1) `raw` must be a handle the C API returned for this type and that is still live. An owned wrapper
    /// destroys the handle on drop, so nothing else may destroy it; a borrowed (`Copy`) wrapper must not be used past
    /// the call that lent the handle.
    #[inline]
    pub unsafe fn from_raw(raw: duckdb_v2_vector_handle) -> Vector {
        Vector { raw }
    }

    #[inline]
    pub fn as_raw(&self) -> duckdb_v2_vector_handle {
        self.raw
    }

    pub fn logical_type(&self) -> Result<LogicalType> {
        Ok(api_call_out!(duckdb_v2_vector_get_logical_type(self.raw) -> LogicalType::from_raw))
    }

    #[inline]
    pub fn vector_type(&self) -> Result<VectorType> {
        let mut vt = DUCKDB_V2_VECTOR_TYPE_OTHER;
        api_call!(duckdb_v2_vector_get_vector_type(self.raw, &mut vt));
        Ok(vt)
    }

    /// The number of rows.
    #[inline]
    pub fn size(&self) -> Result<usize> {
        let mut size: idx_t = 0;
        api_call!(duckdb_v2_vector_get_size(self.raw, &mut size));
        Ok(size as usize)
    }

    /// Sets the number of rows (a chunk column sets the chunk's row count).
    #[inline]
    pub fn set_size(&self, size: usize) -> Result<()> {
        api_call!(duckdb_v2_vector_set_size(self.raw, size as idx_t));
        Ok(())
    }

    /// A unified read view (data, validity, selection, count) over any vector type. A vector of type OTHER
    /// (sequence, FSST, shredded) is flattened in place first, which is what the C API requires before viewing it.
    pub fn view(&self) -> Result<VectorView> {
        if self.vector_type()? == DUCKDB_V2_VECTOR_TYPE_OTHER {
            self.flatten()?;
        }
        let mut view = duckdb_v2_vector_view {
            data: std::ptr::null(),
            validity: std::ptr::null(),
            sel: std::ptr::null(),
            count: 0,
            data_count: 0,
        };
        api_call!(duckdb_v2_vector_get_view(self.raw, &mut view));
        if view.data_count == 0 {
            // The public engine's `duckdb_v2_vector_view` has four members (data, validity, sel, count): it zeroes
            // its own 32 bytes and never writes `data_count`, which is the fifth field here. Derive the bound of the
            // physical indices the way the engine fills the view: a FLAT vector (`sel` null) addresses `count`
            // entries; a CONSTANT vector hands out the zero selection vector and a DICTIONARY one its selection, so the
            // largest selected index plus one bounds the entries every row can reach (1 for a constant).
            view.data_count = if view.sel.is_null() {
                view.count
            } else if view.count == 0 {
                0
            } else {
                // SAFETY(ffi L8.I3): the C API's `duckdb_v2_vector_get_view` contract: a non-null `sel` holds `count`
                // indices, alive as long as the vector the view was read from.
                let sel = unsafe { std::slice::from_raw_parts(view.sel, view.count as usize) };
                sel.iter().copied().max().map_or(0, |max| max as idx_t + 1)
            };
        }
        Ok(VectorView { raw: view })
    }

    /// Materializes the vector as FLAT.
    pub fn flatten(&self) -> Result<()> {
        api_call!(duckdb_v2_vector_flatten(self.raw));
        Ok(())
    }

    /// The writable data of a FLAT vector, as elements of `T` (the C layout of the logical type: `i32`
    /// for INTEGER, `duckdb_v2_bytes` for VARCHAR, `duckdb_v2_list_entry` for LIST, ...).
    #[inline]
    pub fn data_mut<T>(&self) -> Result<*mut T> {
        let mut data: *mut c_void = std::ptr::null_mut();
        api_call!(duckdb_v2_vector_get_data_mutable(self.raw, &mut data));
        Ok(data as *mut T)
    }

    /// The writable validity mask of a FLAT vector (allocated on first use): bit `row % 64` of word
    /// `row / 64` set means valid.
    #[inline]
    pub fn validity_mut(&self) -> Result<*mut u64> {
        let mut validity: *mut u64 = std::ptr::null_mut();
        api_call!(duckdb_v2_vector_flat_get_validity_mutable(self.raw, &mut validity));
        Ok(validity)
    }

    /// Marks `row` NULL (recursively for nested types).
    pub fn set_null(&self, row: usize) -> Result<()> {
        api_call!(duckdb_v2_vector_set_null(self.raw, row as idx_t));
        Ok(())
    }

    pub fn value(&self, row: usize) -> Result<Value> {
        Ok(api_call_out!(duckdb_v2_vector_get_value(self.raw, row as idx_t) -> Value::from_raw))
    }

    pub fn set_value(&self, row: usize, value: &Value) -> Result<()> {
        api_call!(duckdb_v2_vector_set_value(self.raw, row as idx_t, value.as_raw()));
        Ok(())
    }

    /// The number of child vectors: 1 for LIST / MAP / ARRAY, the field count for STRUCT, the member count plus the
    /// tag for UNION, 4 for VARIANT (its canonical `keys` / `children` / `values` / `data`), 0 otherwise.
    pub fn child_count(&self) -> Result<usize> {
        let mut count: idx_t = 0;
        api_call!(duckdb_v2_vector_get_child_count(self.raw, &mut count));
        Ok(count as usize)
    }

    #[inline]
    pub fn child(&self, index: usize) -> Result<Vector> {
        let mut raw: duckdb_v2_vector_handle = std::ptr::null_mut();
        api_call!(duckdb_v2_vector_get_child(self.raw, index as idx_t, &mut raw));
        Ok(Vector { raw })
    }

    /// Makes this vector reference `other`'s data.
    pub fn reference(&self, other: &Vector) -> Result<()> {
        api_call!(duckdb_v2_vector_reference(self.raw, other.raw));
        Ok(())
    }

    /// Turns this VARIANT vector into a SHREDDED vector over `shredded_data`, a `STRUCT(unshredded, shredded)` vector
    /// of `count` rows whose buffers it references from here on (`duckdb_v2_vector_shred`).
    pub fn shred(&self, shredded_data: &Vector, count: usize) -> Result<()> {
        api_call!(duckdb_v2_vector_shred(self.raw, shredded_data.raw, count as idx_t));
        Ok(())
    }

    /// Turns this vector into a DICTIONARY vector of `count` rows over a reusable dictionary: row `i` is entry
    /// `sel[i]` (`duckdb_v2_vector_dictionary`). The vector references the entries, so the dictionary may be dropped
    /// afterwards; its type must equal the vector's and every index must be below its size.
    pub fn dictionary(&self, dictionary: &Dictionary, sel: &[duckdb_v2_sel_t], count: usize) -> Result<()> {
        assert!(sel.len() >= count, "the selection must hold an index per row");
        api_call!(duckdb_v2_vector_dictionary(self.raw, dictionary.raw, sel.as_ptr(), count as idx_t));
        Ok(())
    }

    /// Turns the vector into a CONSTANT vector of `value` with `count` rows.
    pub fn make_constant(&self, value: &Value, count: usize) -> Result<()> {
        api_call!(duckdb_v2_vector_make_constant(self.raw, value.as_raw(), count as idx_t));
        Ok(())
    }

    /// Fills a BIGINT-typed vector with `start, start + increment, ...` over `count` rows.
    pub fn make_sequence(&self, start: i64, increment: i64, count: usize) -> Result<()> {
        api_call!(duckdb_v2_vector_make_sequence(self.raw, start, increment, count as idx_t));
        Ok(())
    }

    /// Sets the validity of a CONSTANT vector.
    pub fn constant_set_valid(&self, valid: bool) -> Result<()> {
        api_call!(duckdb_v2_vector_constant_set_valid(self.raw, valid));
        Ok(())
    }

    /// Keeps `data` alive for as long as this string-backed vector's heap (`duckdb_v2_vector_add_auxiliary_data`,
    /// `StringVector::AddAuxiliaryData`): the out-of-line string entries written into the vector may then point into
    /// `data` (`bytes_ref`) instead of being copied into its arena. The data is dropped - on any thread - once the heap
    /// goes; on failure it is dropped here.
    pub fn add_auxiliary_data<T: Send + Sync + 'static>(&self, data: Box<T>) -> Result<()> {
        /// `duckdb_v2_opaque_destroy_fn`: the engine releases the data through this once.
        unsafe extern "C" fn destroy<T>(data: *mut c_void) {
            if !data.is_null() {
                // SAFETY(ffi): `data` is the `Box<T>` `add_auxiliary_data` leaked for the engine, which destroys it once.
                drop(unsafe { Box::from_raw(data.cast::<T>()) });
            }
        }
        let raw = Box::into_raw(data);
        let result = (|| -> Result<()> {
            api_call!(duckdb_v2_vector_add_auxiliary_data(self.raw, raw.cast::<c_void>(), Some(destroy::<T>)));
            Ok(())
        })();
        if result.is_err() {
            // not taken over by the engine (the header's contract on failure)
            // SAFETY(ffi): `raw` came from `Box::into_raw` above and the engine did not take it.
            drop(unsafe { Box::from_raw(raw) });
        }
        result
    }

    /// Whether the host can keep a producer's buffers alive with a vector ([`Vector::add_auxiliary_data`]); without
    /// the entry a producer copies its strings into the vector's arena.
    pub fn auxiliary_data_available() -> bool {
        crate::api::api().duckdb_v2_vector_add_auxiliary_data.is_some()
    }

    /// The arena strings written into this vector are allocated from.
    #[inline]
    pub fn arena(&self) -> Result<Arena> {
        let mut raw: duckdb_v2_arena_handle = std::ptr::null_mut();
        api_call!(duckdb_v2_vector_get_arena(self.raw, &mut raw));
        Ok(Arena { raw })
    }

    /// Writes string bytes into row `row` of a FLAT VARCHAR/BLOB vector: inlined when they fit the
    /// 12-byte prefix, else copied into the vector's arena. The vector is grown to hold the row when it holds fewer
    /// (a chunk column sets the chunk's row count). One engine call per row for the size and one for the data: a loop
    /// over many rows uses a [`StringWriter`].
    pub fn write_bytes(&self, row: usize, bytes: &[u8]) -> Result<()> {
        // grown to hold the row when it holds fewer (a chunk column sets the chunk's row count, as `flat_data_mut`
        // and `StringWriter` do): the entries up to `count` exist afterwards
        let mut count = self.size()?;
        if row >= count {
            count = row + 1;
            self.set_size(count)?;
        }
        let data = self.data_mut::<duckdb_v2_bytes>()?;
        let entry = make_bytes(self, bytes)?;
        // SAFETY(ffi L8.I3): the C API's contract: `data` (`get_data_mutable`, a FLAT vector) addresses the vector's
        // `count` entries of `string_t`; `Cell` has the entry's layout, and the callback is the one writer of the vector
        // for its duration, so the shared cells alias nothing else.
        let slots = unsafe { std::slice::from_raw_parts(data as *const std::cell::Cell<duckdb_v2_bytes>, count) };
        slots[row].set(entry);
        Ok(())
    }

    /// [`Vector::write_bytes`] for text.
    pub fn write_str(&self, row: usize, text: &str) -> Result<()> {
        self.write_bytes(row, text.as_bytes())
    }
}

/// Builds a `string_t` for `bytes`, allocating from the vector's arena when they do not inline.
pub fn make_bytes(vector: &Vector, bytes: &[u8]) -> Result<duckdb_v2_bytes> {
    if bytes.len() <= DUCKDB_V2_BYTES_INLINE_LENGTH as usize {
        return Ok(bytes_inlined(bytes));
    }
    // the copy lives as long as the arena, which the vector's strings point into
    Ok(bytes_pointer(vector.arena()?.copy_bytes(bytes)?))
}

/// The inlined `string_t` of `bytes` (`bytes.len() <= DUCKDB_V2_BYTES_INLINE_LENGTH`): the bytes live inside the
/// entry (one memcpy, as `string_t`'s constructor copies).
fn bytes_inlined(bytes: &[u8]) -> duckdb_v2_bytes {
    let mut inlined = [0u8; DUCKDB_V2_BYTES_INLINE_LENGTH as usize];
    inlined[..bytes.len()].copy_from_slice(bytes);
    let inlined =
        duckdb_v2_bytes_inlined { length: bytes.len() as u32, inlined: inlined.map(|b| b as std::ffi::c_char) };
    duckdb_v2_bytes { value: duckdb_v2_bytes_value { inlined } }
}

/// The out-of-line `string_t` of `bytes` (`bytes.len() > DUCKDB_V2_BYTES_INLINE_LENGTH`): the 4-byte prefix plus a
/// pointer at the bytes, which the entry references for as long as it is read - the caller states where they live.
fn bytes_pointer(bytes: &[u8]) -> duckdb_v2_bytes {
    let mut prefix = [0u8; 4];
    prefix.copy_from_slice(&bytes[..4]);
    let pointer = duckdb_v2_bytes_pointer {
        length: bytes.len() as u32,
        prefix: prefix.map(|b| b as std::ffi::c_char),
        ptr: bytes.as_ptr() as *mut std::ffi::c_char,
    };
    duckdb_v2_bytes { value: duckdb_v2_bytes_value { pointer } }
}

/// The bytes a `string_t` refers to.
///
/// # Safety
/// (layout L5.I6) L8.I3 / L5.I6: `bytes` is a `string_t` entry of a live vector; a non-inlined one points at `length`
/// bytes kept alive by that vector's heap (or a buffer it references) for `'a`.
pub unsafe fn bytes_as_slice(bytes: &duckdb_v2_bytes) -> &[u8] {
    let length = bytes.length() as usize;
    if length <= DUCKDB_V2_BYTES_INLINE_LENGTH as usize {
        // SAFETY(layout L5.I6): inlined bytes live inside the struct.
        unsafe { std::slice::from_raw_parts(bytes.value.inlined.inlined.as_ptr().cast::<u8>(), length) }
    } else {
        // SAFETY(layout L5.I6): a non-inlined string_t points at `length` live bytes.
        unsafe { std::slice::from_raw_parts(bytes.value.pointer.ptr as *const u8, length) }
    }
}

/// The arena of a vector (borrowed).
#[derive(Clone, Copy)]
pub struct Arena {
    raw: duckdb_v2_arena_handle,
}

impl Arena {
    pub fn as_raw(&self) -> duckdb_v2_arena_handle {
        self.raw
    }

    /// `byte_len` uninitialized bytes that live as long as the arena.
    pub fn allocate(&self, byte_len: usize) -> Result<*mut u8> {
        let mut ptr: *mut u8 = std::ptr::null_mut();
        api_call!(duckdb_v2_arena_allocate(self.raw, byte_len as idx_t, &mut ptr));
        Ok(ptr)
    }
}

/// An owned reusable dictionary (`duckdb_v2_dictionary_handle`, `DictionaryVector::CreateReusableDictionary`): a
/// FLAT vector of a fixed number of entries that DICTIONARY vectors select their rows from ([`Vector::dictionary`]).
/// Fill the entries through [`Dictionary::entries`] before the first vector is made over them; a NULL row of a
/// dictionary vector selects an entry marked NULL in the entries' validity. The vectors made over the dictionary keep
/// the entries alive after it is dropped.
pub struct Dictionary {
    raw: duckdb_v2_dictionary_handle,
}

// SAFETY(protocol L8.I8): (T5, L8.I1) an owned `duckdb_v2_dictionary_handle` (`duckdb_v2.h`: a producer "fills its
// entries once through `duckdb_v2_dictionary_get_vector()`, and references it from every output vector"); the
// dictionary has no thread affinity, so it moves between threads as a whole. Not `Sync`: `entries(&self)` lends the
// writable entry vector, so the dictionary is filled by one thread at a time (the vectors made over it read it through
// the engine afterwards).
unsafe impl Send for Dictionary {}

impl Dictionary {
    /// A dictionary of `size` zero-initialized, valid entries of `type_`, allocated through the factory's database.
    /// STRUCT types are rejected: struct vectors cannot be dictionaries.
    pub fn new(factory: Factory, type_: &LogicalType, size: usize) -> Result<Dictionary> {
        let mut raw: duckdb_v2_dictionary_handle = std::ptr::null_mut();
        api_call!(duckdb_v2_dictionary_create(factory.as_raw(), type_.as_raw(), size as idx_t, &mut raw));
        Ok(Dictionary { raw })
    }

    pub fn as_raw(&self) -> duckdb_v2_dictionary_handle {
        self.raw
    }

    /// The borrowed vector of the entries (`duckdb_v2_dictionary_get_vector`): FLAT, of the dictionary's size,
    /// which cannot be changed.
    pub fn entries(&self) -> Result<Vector> {
        let mut raw: duckdb_v2_vector_handle = std::ptr::null_mut();
        api_call!(duckdb_v2_dictionary_get_vector(self.raw, &mut raw));
        Ok(Vector { raw })
    }
}

impl Drop for Dictionary {
    fn drop(&mut self) {
        api_destroy!(duckdb_v2_dictionary_destroy(&mut self.raw));
    }
}

/// A unified read view over a vector: `data[sel[row]]` is row `row`'s element. `data` holds `data_count` entries - the
/// C API's bound of every physical index: the row count of a FLAT vector, 1 for a CONSTANT one, the dictionary's
/// entry count for a DICTIONARY one -, `validity` one bit per entry, `sel` one index per row.
#[derive(Clone, Copy)]
pub struct VectorView {
    raw: duckdb_v2_vector_view,
}

impl VectorView {
    pub fn raw(&self) -> &duckdb_v2_vector_view {
        &self.raw
    }

    /// The number of logical rows.
    #[inline]
    pub fn count(&self) -> usize {
        self.raw.count as usize
    }

    /// The number of entries of `data` (and of the rows `validity` covers): the bound of every physical index.
    #[inline]
    pub fn data_count(&self) -> usize {
        self.raw.data_count as usize
    }

    /// The selection: one physical index per row, `None` for a FLAT view (the physical index is the row).
    #[inline]
    pub fn selection(&self) -> Option<&[duckdb_v2_sel_t]> {
        if self.raw.sel.is_null() {
            return None;
        }
        // SAFETY(ffi L8.I3): the C API's `duckdb_v2_vector_get_view` contract: a non-null `sel` holds `count` indices,
        // each below `data_count`, alive as long as the vector the view was read from.
        Some(unsafe { std::slice::from_raw_parts(self.raw.sel, self.count()) })
    }

    /// The validity words, one bit per entry (`data_count.div_ceil(64)` words); `None` when every entry is valid.
    #[inline]
    pub fn validity(&self) -> Option<&[u64]> {
        if self.raw.validity.is_null() {
            return None;
        }
        // SAFETY(ffi L8.I3): the C API's view contract: a non-null mask covers the `data_count` entries (the engine
        // allocates it over the vector's capacity), one bit per entry in 64-bit words, alive as long as the vector.
        Some(unsafe { std::slice::from_raw_parts(self.raw.validity, self.data_count().div_ceil(64)) })
    }

    /// The physical index of logical row `row`. Panics when the view has a selection and `row` is not below
    /// [`VectorView::count`].
    #[inline]
    pub fn index(&self, row: usize) -> usize {
        match self.selection() {
            None => row,
            Some(sel) => sel[row] as usize,
        }
    }

    /// Whether logical row `row` is valid (not NULL). Panics when `row` maps outside the entries of a view with a
    /// validity mask.
    #[inline]
    pub fn is_valid(&self, row: usize) -> bool {
        match self.validity() {
            None => true,
            Some(words) => {
                let index = self.index(row);
                (words[index / 64] >> (index % 64)) & 1 == 1
            }
        }
    }

    /// Whether any row can be NULL.
    pub fn can_have_null(&self) -> bool {
        !self.raw.validity.is_null()
    }

    /// The validity words covering the first `rows` rows of a view without a selection (a flattened vector, whose
    /// physical index is the logical row), `None` when every row is valid. Panics when the view has a selection or
    /// fewer than `rows` rows.
    pub fn validity_words(&self, rows: usize) -> Option<&[u64]> {
        assert!(self.raw.sel.is_null(), "validity_words: the view has a selection vector");
        assert!(rows <= self.count(), "validity_words: {rows} rows requested of a view of {} rows", self.count());
        if rows == 0 {
            return None;
        }
        // without a selection the first `rows <= count <= data_count` rows are the first `rows` entries
        self.validity().map(|words| &words[..rows.div_ceil(64)])
    }

    /// The elements as `T`, indexed by physical position (use [`VectorView::index`] to map rows); the raw pointer for
    /// a producer that hands the buffer on (the parquet writer). The checked forms are [`TypedView`] and
    /// [`StringView`].
    pub fn data<T>(&self) -> *const T {
        self.raw.data as *const T
    }
}

// -------------------------------------------------------------------------------------------------
// Typed, bounded access
// -------------------------------------------------------------------------------------------------

/// A plain-data element type of a vector's data array: every bit pattern of `size_of::<Self>()` bytes is a
/// valid value, so reading an element out of an engine buffer can never produce an invalid value, and writing one
/// never leaves padding the engine reads back.
///
/// # Safety
/// (layout L8.I3) Implementors must have no invalid bit patterns and no padding (integers, floats, and `#[repr(C)]`
/// aggregates of them); `bool` is deliberately not one (an engine BOOLEAN vector is read as `u8`).
pub unsafe trait Element: Copy + 'static {}

macro_rules! plain_elements {
    ($($t:ty),* $(,)?) => {
        $(
            // SAFETY(layout L8.I3): integers, floats and the `#[repr(C)]` C API aggregates of them have no invalid bit
            // patterns and no padding.
            unsafe impl Element for $t {}
        )*
    };
}
plain_elements!(
    i8,
    i16,
    i32,
    i64,
    i128,
    u8,
    u16,
    u32,
    u64,
    u128,
    f32,
    f64,
    [u64; 2],
    [i64; 2],
    [i32; 4],
    [u8; 16],
    duckdb_v2_bytes,
    duckdb_v2_list_entry,
    duckdb_v2_hugeint_t,
    duckdb_v2_uhugeint_t,
    duckdb_v2_interval_t,
);

/// Checks that `T` is laid out like the elements of `type_` (the width of the physical type) and that `data` is
/// aligned for it, so that a slice of `T` over the vector's buffer is well-formed.
fn check_element<T: Element>(type_: &LogicalType, data: *const c_void, what: &str) -> Result<()> {
    match type_.element_width()? {
        Some(width) if width == std::mem::size_of::<T>() => {}
        Some(width) => {
            return Err(Error::invalid_input(format!(
                "{what}: a {} vector holds {width}-byte elements, not {}-byte ones",
                type_.to_text()?,
                std::mem::size_of::<T>()
            )));
        }
        None => {
            return Err(Error::invalid_input(format!(
                "{what}: a {} vector has no element array of its own (its rows live in its children)",
                type_.to_text()?
            )));
        }
    }
    if !data.is_null() && !(data as usize).is_multiple_of(std::mem::align_of::<T>()) {
        return Err(Error::internal(format!(
            "{what}: the vector's buffer is not aligned for {}",
            std::any::type_name::<T>()
        )));
    }
    Ok(())
}

/// Whether `id` is one of the string-backed types (16-byte `string_t` entries pointing at bytes).
fn is_string_type(id: TypeId) -> bool {
    matches!(
        id,
        DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR
            | DUCKDB_V2_LOGICAL_TYPE_ID_BLOB
            | DUCKDB_V2_LOGICAL_TYPE_ID_BIT
            | DUCKDB_V2_LOGICAL_TYPE_ID_BIGNUM
            | DUCKDB_V2_LOGICAL_TYPE_ID_GEOMETRY
    )
}

/// A read view over a vector whose elements are known to be `T` ([`Vector::typed_view`]): the element type was
/// checked against the vector's logical type once, so every read is a plain, bounds-checked load.
pub struct TypedView<T: Element> {
    view: VectorView,
    /// The view's `data` (checked against `T`), or a dangling pointer when the vector exposes no data; `data_count`
    /// the entries it addresses (0 then): the one bound every physical index is checked against.
    data: std::ptr::NonNull<T>,
    data_count: usize,
    _element: std::marker::PhantomData<T>,
}

impl<T: Element> TypedView<T> {
    /// A typed view over `view`, whose elements the caller checked to be `T` (`check_element`).
    fn new(view: VectorView) -> TypedView<T> {
        let (data, data_count) = match std::ptr::NonNull::new(view.raw.data as *mut T) {
            Some(data) => (data, view.data_count()),
            None => (std::ptr::NonNull::dangling(), 0),
        };
        TypedView { view, data, data_count, _element: std::marker::PhantomData }
    }
}

impl<T: Element> Clone for TypedView<T> {
    fn clone(&self) -> TypedView<T> {
        *self
    }
}
impl<T: Element> Copy for TypedView<T> {}

impl<T: Element> TypedView<T> {
    /// The untyped view (row mapping and validity).
    pub fn view(&self) -> &VectorView {
        &self.view
    }

    /// The number of logical rows.
    #[inline]
    pub fn count(&self) -> usize {
        self.view.count()
    }

    /// Whether any row can be NULL.
    #[inline]
    pub fn can_have_null(&self) -> bool {
        self.view.can_have_null()
    }

    #[inline]
    fn check(&self, row: usize) {
        assert!(row < self.view.count(), "row {row} out of range of a vector of {} rows", self.view.count());
    }

    /// The entries, indexed by physical position ([`VectorView::index`] maps rows): `data_count` of them, none when
    /// the vector exposes no data (an output vector not written yet).
    #[inline]
    pub fn entries(&self) -> &[T] {
        // SAFETY(ffi L8.I3): the C API's `duckdb_v2_vector_get_view` contract: `data` addresses `data_count` entries of
        // the vector's physical type, whose width and alignment `Vector::typed_view` / `string_view` checked against `T`
        // (`check_element`), every bit pattern a `T` (`Element`); `TypedView::new` kept that pointer and count (or a
        // dangling pointer and 0 without data), and the entries live as long as the vector the view was read from.
        unsafe { std::slice::from_raw_parts(self.data.as_ptr(), self.data_count) }
    }

    /// Whether logical row `row` is valid (not NULL). Panics when `row` is not below [`TypedView::count`].
    #[inline]
    pub fn is_valid(&self, row: usize) -> bool {
        self.check(row);
        self.view.is_valid(row)
    }

    /// The element of logical row `row`, whatever its validity. Panics when `row` is not below [`TypedView::count`]
    /// (the entry index of a FLAT view, the selection's bound of a DICTIONARY one).
    #[inline]
    pub fn get(&self, row: usize) -> T {
        self.entries()[self.view.index(row)]
    }

    /// The element of logical row `row`, or `None` for a NULL row.
    #[inline]
    pub fn get_valid(&self, row: usize) -> Option<T> {
        if self.is_valid(row) { Some(self.get(row)) } else { None }
    }
}

/// A read view over a VARCHAR / BLOB / BIT / BIGNUM / GEOMETRY (or aliased, e.g. JSON) vector: `string_t` entries
/// ([`Vector::string_view`]).
#[derive(Clone, Copy)]
pub struct StringView {
    inner: TypedView<duckdb_v2_bytes>,
}

impl StringView {
    pub fn view(&self) -> &VectorView {
        &self.inner.view
    }

    #[inline]
    pub fn count(&self) -> usize {
        self.inner.count()
    }

    #[inline]
    pub fn can_have_null(&self) -> bool {
        self.inner.can_have_null()
    }

    /// Whether logical row `row` is valid (not NULL). Panics when `row` is out of range.
    #[inline]
    pub fn is_valid(&self, row: usize) -> bool {
        self.inner.is_valid(row)
    }

    /// The `string_t` of logical row `row` (the bytes it points at are the vector's).
    #[inline]
    pub fn entry(&self, row: usize) -> duckdb_v2_bytes {
        self.inner.get(row)
    }

    /// The string bytes of logical row `row`, whatever its validity (a NULL row reads as the bytes its slot holds,
    /// usually empty). Panics when `row` is out of range.
    #[inline]
    pub fn bytes(&self, row: usize) -> &[u8] {
        let entry = &self.inner.entries()[self.inner.view.index(row)];
        // SAFETY(layout L5.I6): the entry is a `string_t` of the vector (`Vector::string_view` checked the type): a
        // non-inlined one points at `length` bytes the vector's heap (or a buffer it references) keeps alive as long as
        // the vector.
        unsafe { bytes_as_slice(entry) }
    }

    /// The string bytes of logical row `row`, or `None` for a NULL row: one range check and one row mapping for
    /// the `is_valid` + `bytes` pair of a scan loop. Panics when `row` is out of range.
    #[inline]
    pub fn valid_bytes(&self, row: usize) -> Option<&[u8]> {
        let view = &self.inner.view;
        let index = view.index(row);
        if let Some(words) = view.validity()
            && (words[index / 64] >> (index % 64)) & 1 == 0
        {
            return None;
        }
        let entry = &self.inner.entries()[index];
        // SAFETY(layout L5.I6): as for `bytes` - the entry is a `string_t` of the vector, whose bytes live as long as it.
        Some(unsafe { bytes_as_slice(entry) })
    }

    /// The text of logical row `row` (`""` for bytes that are not UTF-8).
    #[inline]
    pub fn str(&self, row: usize) -> &str {
        std::str::from_utf8(self.bytes(row)).unwrap_or("")
    }
}

/// A view over the elements of a LIST / MAP vector: `(offset, length)` into the child vector.
pub type ListView = TypedView<duckdb_v2_list_entry>;

/// The writable validity mask of a FLAT vector over its first `count` rows ([`Vector::flat_validity_mut`]): a set
/// bit is a valid row.
pub struct ValidityMut<'a> {
    words: *mut u64,
    count: usize,
    _vector: std::marker::PhantomData<&'a Vector>,
}

impl ValidityMut<'_> {
    /// The number of rows the mask covers.
    pub fn count(&self) -> usize {
        self.count
    }

    #[inline]
    fn check(&self, row: usize) {
        assert!(row < self.count, "row {row} out of range of a validity mask of {} rows", self.count);
    }

    /// The words as cells: written through `&self`, like the engine's own mask writes.
    #[inline]
    fn cells(&self) -> &[std::cell::Cell<u64>] {
        // SAFETY(ffi L8.I3): the C API's contract: the mask (`flat_get_validity_mutable`) covers the vector's capacity,
        // which holds `count` rows after `set_size` (`Vector::flat_validity_mut`), so `count.div_ceil(64)` words exist;
        // `Cell<u64>` has `u64`'s layout, and the vector is lent to the running callback, which holds the only writer,
        // so the shared cells alias nothing else.
        unsafe { std::slice::from_raw_parts(self.words as *const std::cell::Cell<u64>, self.count.div_ceil(64)) }
    }

    /// Marks `row` NULL. Panics when `row` is not below [`ValidityMut::count`].
    #[inline]
    pub fn set_invalid(&self, row: usize) {
        self.check(row);
        let word = &self.cells()[row / 64];
        word.set(word.get() & !(1u64 << (row % 64)));
    }

    /// Marks `row` valid. Panics when `row` is not below [`ValidityMut::count`].
    #[inline]
    pub fn set_valid(&self, row: usize) {
        self.check(row);
        let word = &self.cells()[row / 64];
        word.set(word.get() | (1u64 << (row % 64)));
    }

    #[inline]
    pub fn is_valid(&self, row: usize) -> bool {
        self.check(row);
        (self.cells()[row / 64].get() >> (row % 64)) & 1 == 1
    }

    /// Sets the validity of `row`.
    #[inline]
    pub fn set(&self, row: usize, valid: bool) {
        if valid { self.set_valid(row) } else { self.set_invalid(row) }
    }

    /// Marks every row of the mask NULL (`SetAllInvalid`).
    pub fn set_all_invalid(&mut self) {
        for word in self.words_mut() {
            *word = 0;
        }
    }

    /// The mask words covering the rows: bit `row % 64` of word `row / 64`. Bits past `count` in the last word
    /// belong to rows the vector does not hold; leave them set.
    pub fn words_mut(&mut self) -> &mut [u64] {
        let len = self.count.div_ceil(64);
        // SAFETY(ffi L8.I3): as for `cells`, exclusively: `&mut self` is the one writer for the slice's lifetime.
        unsafe { std::slice::from_raw_parts_mut(self.words, len) }
    }
}

impl Vector {
    /// The vector's type, element width and FLAT-ness checked for a typed access of `count` rows: makes sure the
    /// vector holds at least `count` rows (`set_size` when it holds fewer), so that `count` elements of its data
    /// array exist.
    fn prepare_flat<T: Element>(&self, count: usize, what: &str) -> Result<LogicalType> {
        if self.vector_type()? != DUCKDB_V2_VECTOR_TYPE_FLAT {
            return Err(Error::invalid_input(format!("{what}: the vector is not FLAT (flatten it first)")));
        }
        let type_ = self.logical_type()?;
        check_element::<T>(&type_, std::ptr::null(), what)?;
        if self.size()? < count {
            self.set_size(count)?;
        }
        Ok(type_)
    }

    /// A typed read view: checks once that the vector's elements are laid out like `T` (see [`Element`]), then
    /// every row is a bounds-checked load ([`TypedView::get`]). A vector of type OTHER is flattened first, as
    /// [`Vector::view`] does.
    pub fn typed_view<T: Element>(&self) -> Result<TypedView<T>> {
        let type_ = self.logical_type()?;
        let view = self.view()?;
        check_element::<T>(&type_, view.raw.data, "typed_view")?;
        Ok(TypedView::new(view))
    }

    /// A read view over the `string_t` entries of a string-backed vector (VARCHAR, BLOB, BIT, BIGNUM, GEOMETRY and
    /// their aliases).
    pub fn string_view(&self) -> Result<StringView> {
        let type_ = self.logical_type()?;
        if !is_string_type(type_.id()?) {
            return Err(Error::invalid_input(format!("string_view: a {} vector holds no strings", type_.to_text()?)));
        }
        let view = self.view()?;
        check_element::<duckdb_v2_bytes>(&type_, view.raw.data, "string_view")?;
        Ok(StringView { inner: TypedView::new(view) })
    }

    /// A read view over the `(offset, length)` entries of a LIST / MAP vector.
    pub fn list_view(&self) -> Result<ListView> {
        let type_ = self.logical_type()?;
        if !matches!(type_.id()?, DUCKDB_V2_LOGICAL_TYPE_ID_LIST | DUCKDB_V2_LOGICAL_TYPE_ID_MAP) {
            return Err(Error::invalid_input(format!(
                "list_view: a {} vector holds no list entries",
                type_.to_text()?
            )));
        }
        self.typed_view::<duckdb_v2_list_entry>()
    }

    /// The first `count` elements of a FLAT vector, writable, as `T` (checked against the vector's type, see
    /// [`Element`]). The vector is grown to hold `count` rows when it holds fewer (a chunk column sets the chunk's
    /// row count, a LIST child reserves). The slice aliases the engine's buffer: it is the one writer of the vector
    /// for the duration of the callback, so do not hold two at once.
    ///
    /// A table function that asks for the chunk's capacity and then writes fewer rows must `set_size` the chunk to
    /// the rows written on every path out of the callback - the engine reads the chunk's size, and a `count` left
    /// behind is `count` rows of unwritten data (an in-out function that returns an unsized chunk never ends).
    #[expect(
        clippy::mut_from_ref,
        reason = "the callback is the one writer of the vector for its duration (see the docs: do not hold two at once); issue #2 §1b"
    )]
    pub fn flat_data_mut<T: Element>(&self, count: usize) -> Result<&mut [T]> {
        self.prepare_flat::<T>(count, "flat_data_mut")?;
        if count == 0 {
            return Ok(&mut []);
        }
        let data = self.data_mut::<T>()?;
        if !(data as usize).is_multiple_of(std::mem::align_of::<T>()) {
            return Err(Error::internal(format!(
                "flat_data_mut: the vector's buffer is not aligned for {}",
                std::any::type_name::<T>()
            )));
        }
        // SAFETY(layout L8.I3): the vector is FLAT and holds at least `count` rows of `size_of::<T>()`-byte elements
        // (`prepare_flat`), its buffer is aligned for `T`, and the engine lends it to this callback for writing.
        Ok(unsafe { std::slice::from_raw_parts_mut(data, count) })
    }

    /// The writable validity mask of a FLAT vector over its first `count` rows (allocated all-valid on first use;
    /// the vector is grown to hold `count` rows when it holds fewer).
    pub fn flat_validity_mut(&self, count: usize) -> Result<ValidityMut<'_>> {
        if self.vector_type()? != DUCKDB_V2_VECTOR_TYPE_FLAT {
            return Err(Error::invalid_input("flat_validity_mut: the vector is not FLAT (flatten it first)"));
        }
        if self.size()? < count {
            self.set_size(count)?;
        }
        let words = self.validity_mut()?;
        // The view is sound because the engine allocates the mask over the vector's capacity, which holds at least
        // `count` rows after `set_size`; the pointer is non-null (the call allocates on demand).
        Ok(ValidityMut { words, count, _vector: std::marker::PhantomData })
    }

    /// Casts this vector into `target` with the client's cast set (`VectorOperations::Cast(context, ...)`,
    /// `duckdb_v2_vector_cast`): `Ok((converted, message))` - `converted` is false when a `CastMode::TRY` cast
    /// left rows NULL, `message` the error text of the failed rows (empty when all converted); a failing regular
    /// cast is an `Err`.
    pub fn cast_into(&self, factory: Factory, target: &Vector, mode: CastMode) -> Result<(bool, String)> {
        let mut converted = true;
        let mut cast_err: duckdb_v2_error_info_handle = std::ptr::null_mut();
        api_call!(duckdb_v2_vector_cast(factory.as_raw(), self.raw, target.raw, mode, &mut converted, &mut cast_err));
        Ok((converted, crate::error::take_message(cast_err)))
    }

    /// [`Vector::cast_into`] through the built-in casts only (`VectorOperations::DefaultCast`,
    /// `duckdb_v2_vector_default_cast`): what a cast an extension registered itself must use when it converts
    /// through the engine's cast for the same type pair, or it recurses into itself. An engine without the entry
    /// (the upstream library) casts with the client's cast set instead; see [`Vector::default_cast_available`].
    pub fn default_cast_into(&self, factory: Factory, target: &Vector, mode: CastMode) -> Result<(bool, String)> {
        if !Vector::default_cast_available() {
            return self.cast_into(factory, target, mode);
        }
        let mut converted = true;
        let mut cast_err: duckdb_v2_error_info_handle = std::ptr::null_mut();
        api_call!(duckdb_v2_vector_default_cast(
            factory.as_raw(),
            self.raw,
            target.raw,
            mode,
            &mut converted,
            &mut cast_err
        ));
        Ok((converted, crate::error::take_message(cast_err)))
    }

    /// Whether the loading engine offers the built-in-only cast (`duckdb_v2_vector_default_cast`).
    pub fn default_cast_available() -> bool {
        crate::api::api().duckdb_v2_vector_default_cast.is_some()
    }
}

impl Arena {
    /// `byte_len` writable, uninitialized bytes that live as long as the arena (the vector's strings may point at
    /// them). Every call hands out fresh memory.
    #[expect(
        clippy::mut_from_ref,
        reason = "every call hands out fresh arena memory (a bump allocator): no two slices alias"
    )]
    pub fn allocate_slice(&self, byte_len: usize) -> Result<&mut [std::mem::MaybeUninit<u8>]> {
        if byte_len == 0 {
            return Ok(&mut []);
        }
        let ptr = self.allocate(byte_len)?;
        // SAFETY(protocol L8.I3): the arena returned `byte_len` writable bytes nobody else refers to; they live as long
        // as the arena.
        Ok(unsafe { std::slice::from_raw_parts_mut(ptr as *mut std::mem::MaybeUninit<u8>, byte_len) })
    }

    /// Copies `bytes` into the arena and returns the copy (it lives as long as the arena).
    pub fn copy_bytes(&self, bytes: &[u8]) -> Result<&[u8]> {
        if bytes.is_empty() {
            return Ok(&[]);
        }
        Ok(self.allocate_slice(bytes.len())?.write_copy_of_slice(bytes))
    }
}

/// A `string_t` that references `bytes` without copying them: inlined when they fit the 12-byte prefix, else a
/// pointer to them with the 4-byte prefix.
///
/// # Safety
/// (protocol L5.I6) For a non-inlined result (`bytes.len() > DUCKDB_V2_BYTES_INLINE_LENGTH`) `bytes` must stay alive
/// and unchanged as long as the vector the entry is written into: the entry is a `string_t` whose pointer the engine
/// follows for every read of the row.
pub unsafe fn bytes_ref(bytes: &[u8]) -> duckdb_v2_bytes {
    if bytes.len() <= DUCKDB_V2_BYTES_INLINE_LENGTH as usize { bytes_inlined(bytes) } else { bytes_pointer(bytes) }
}

/// The first block a [`StringWriter`] takes from the arena; every further block is twice the previous one up to
/// [`STRING_BLOCK_MAX`].
const STRING_BLOCK_MIN: usize = 4 * 1024;
const STRING_BLOCK_MAX: usize = 64 * 1024;

/// A writer for the string slots of a FLAT VARCHAR / BLOB (or aliased) vector of `count` rows.
///
/// Out-of-line strings are carved from blocks the writer takes from the vector's arena (the C++ `StringHeap`
/// hands out its arena memory the same way), so a row costs an engine call only when a block runs out. Rows are
/// addressed by position and bounds-checked against `count`; the writer is the one writer of the vector for the
/// duration of the callback.
pub struct StringWriter {
    data: *mut duckdb_v2_bytes,
    validity: *mut u64,
    count: usize,
    arena: Arena,
    /// The current block: next free byte and bytes left.
    block: std::cell::Cell<(*mut u8, usize)>,
    /// The size of the next block.
    next_block: std::cell::Cell<usize>,
}

impl StringWriter {
    /// A writer over the first `count` rows of `vector` (grown to hold them when it holds fewer); its validity mask
    /// is allocated (all valid) so that [`StringWriter::write_null`] can clear bits.
    pub fn new(vector: &Vector, count: usize) -> Result<StringWriter> {
        let mut writer = StringWriter {
            data: std::ptr::null_mut(),
            validity: std::ptr::null_mut(),
            count: 0,
            arena: vector.arena()?,
            block: std::cell::Cell::new((std::ptr::null_mut(), 0)),
            next_block: std::cell::Cell::new(STRING_BLOCK_MIN),
        };
        writer.refresh(vector, count)?;
        Ok(writer)
    }

    /// Points the writer at the (possibly reallocated) data of its vector over `count` rows, keeping its arena
    /// block (a LIST child grows when its parent reserves more rows).
    pub fn refresh(&mut self, vector: &Vector, count: usize) -> Result<()> {
        let type_ = vector.prepare_flat::<duckdb_v2_bytes>(count, "StringWriter")?;
        if !is_string_type(type_.id()?) {
            return Err(Error::invalid_input(format!("StringWriter: a {} vector holds no strings", type_.to_text()?)));
        }
        self.data = vector.data_mut::<duckdb_v2_bytes>()?;
        self.validity = vector.validity_mut()?;
        self.count = count;
        Ok(())
    }

    /// The number of rows the writer covers.
    pub fn count(&self) -> usize {
        self.count
    }

    /// The arena the out-of-line strings are allocated from.
    pub fn arena(&self) -> Arena {
        self.arena
    }

    #[inline]
    fn check(&self, row: usize) {
        assert!(row < self.count, "row {row} out of range of a string writer over {} rows", self.count);
    }

    /// The string slots as cells: written through `&self`.
    #[inline]
    fn slots(&self) -> &[std::cell::Cell<duckdb_v2_bytes>] {
        // SAFETY(ffi L8.I3): the C API's contract: `refresh` pointed `data` (`get_data_mutable`) at a FLAT string vector
        // of at least `count` rows (`prepare_flat` grew it), so `count` entries exist; `Cell<duckdb_v2_bytes>` has the
        // entry's layout, and the writer is the one writer of the vector for the callback's duration, so the shared
        // cells alias nothing else.
        unsafe { std::slice::from_raw_parts(self.data as *const std::cell::Cell<duckdb_v2_bytes>, self.count) }
    }

    /// The validity words as cells.
    #[inline]
    fn validity_cells(&self) -> &[std::cell::Cell<u64>] {
        // SAFETY(ffi L8.I3): as for `slots`: the mask (`flat_get_validity_mutable`, allocated over the vector's
        // capacity) covers the `count` rows, `count.div_ceil(64)` words.
        unsafe { std::slice::from_raw_parts(self.validity as *const std::cell::Cell<u64>, self.count.div_ceil(64)) }
    }

    /// `len` uninitialized bytes that live as long as the arena, carved from the current block when they fit. Every
    /// call hands out fresh bytes (the block advances past them).
    #[inline]
    #[expect(
        clippy::mut_from_ref,
        reason = "every call hands out fresh arena bytes (a bump allocator over the arena's blocks): no two slices alias"
    )]
    fn alloc(&self, len: usize) -> Result<&mut [std::mem::MaybeUninit<u8>]> {
        let (ptr, left) = self.block.get();
        if len <= left {
            self.block.set((ptr.wrapping_add(len), left - len));
            // SAFETY(protocol L8.I3): `len <= left` bytes remain in the block after `ptr` (the arena returned the
            // block, the writer advanced `ptr` past the bytes it handed out before, and past these just now), so
            // nothing else refers to them; they live as long as the arena.
            return Ok(unsafe { std::slice::from_raw_parts_mut(ptr as *mut std::mem::MaybeUninit<u8>, len) });
        }
        let size = self.next_block.get();
        if len >= size / 4 {
            // a large string gets its own allocation; the block stays for the small ones
            return self.arena.allocate_slice(len);
        }
        let (bytes, rest) = self.arena.allocate_slice(size)?.split_at_mut(len);
        self.block.set((rest.as_mut_ptr().cast::<u8>(), rest.len()));
        self.next_block.set((size * 2).min(STRING_BLOCK_MAX));
        Ok(bytes)
    }

    /// Copies `bytes` into row `row` (inlined when they fit, else into the arena). Panics when `row` is out of
    /// range.
    #[inline]
    pub fn write(&self, row: usize, bytes: &[u8]) -> Result<()> {
        let entry = if bytes.len() <= DUCKDB_V2_BYTES_INLINE_LENGTH as usize {
            bytes_inlined(bytes)
        } else {
            // an out-of-line entry points at arena bytes, which live as long as the vector
            bytes_pointer(self.alloc(bytes.len())?.write_copy_of_slice(bytes))
        };
        self.slots()[row].set(entry);
        Ok(())
    }

    /// [`StringWriter::write`] for text.
    #[inline]
    pub fn write_str(&self, row: usize, text: &str) -> Result<()> {
        self.write(row, text.as_bytes())
    }

    /// References `bytes` from row `row` without copying them.
    ///
    /// # Safety
    /// (protocol L8.I3) `bytes` must stay alive and unchanged as long as the vector (the heap of another vector, a
    /// buffer kept for the vector's lifetime).
    #[inline]
    pub unsafe fn write_ref(&self, row: usize, bytes: &[u8]) {
        // SAFETY(protocol L5.I6): `bytes_ref`'s contract is this fn's: the caller keeps `bytes` alive as long as the vector.
        let entry = unsafe { bytes_ref(bytes) };
        self.slots()[row].set(entry);
    }

    /// Marks row `row` NULL. Panics when `row` is out of range.
    #[inline]
    pub fn write_null(&self, row: usize) {
        self.check(row);
        let word = &self.validity_cells()[row / 64];
        word.set(word.get() & !(1u64 << (row % 64)));
    }

    /// Marks row `row` valid again (rows start out valid).
    #[inline]
    pub fn set_valid(&self, row: usize) {
        self.check(row);
        let word = &self.validity_cells()[row / 64];
        word.set(word.get() | (1u64 << (row % 64)));
    }
}
