//! Kani proof harnesses (`cargo kani -p duckdb-ext-api`, `make kani`) over the pure layout arithmetic of the vector
//! layer: the validity mask bits, the two forms of a `string_t`, and the entry count a view derives from its
//! selection. Nothing here calls the engine: the mask writes into a plain word buffer and the `string_t` forms are
//! built over a plain byte buffer.

use crate::sys::{DUCKDB_V2_BYTES_INLINE_LENGTH, duckdb_v2_sel_t, idx_t};
use crate::vector::{ValidityMut, bytes_as_slice, bytes_inlined, bytes_pointer, derived_data_count};

/// `ValidityMut` over a 32-word buffer for every row count up to 2048 and every row below it: `set_valid` /
/// `set_invalid` / `set` change exactly bit `row % 64` of word `row / 64` and `is_valid` reads it back; nothing
/// outside that bit changes; `set_all_invalid` zeroes exactly the `count.div_ceil(64)` words the rows occupy.
#[kani::proof]
#[kani::unwind(33)]
fn validity_mask_touches_exactly_the_rows_bit() {
    let initial: [u64; 32] = kani::any();
    let count: usize = kani::any();
    kani::assume(count <= 2048);
    let row: usize = kani::any();
    kani::assume(row < count);
    let bit = 1u64 << (row % 64);
    let word = row / 64;

    let mut words = initial;
    let mask = ValidityMut::over_words(&mut words, count);
    assert_eq!(mask.count(), count);
    assert_eq!(mask.is_valid(row), initial[word] & bit != 0);
    mask.set_valid(row);
    assert!(mask.is_valid(row));
    assert_eq!(words[word], initial[word] | bit);
    let mut i = 0;
    while i < 32 {
        assert!(i == word || words[i] == initial[i]);
        i += 1;
    }

    let mut words = initial;
    let mask = ValidityMut::over_words(&mut words, count);
    mask.set_invalid(row);
    assert!(!mask.is_valid(row));
    assert_eq!(words[word], initial[word] & !bit);
    let mut i = 0;
    while i < 32 {
        assert!(i == word || words[i] == initial[i]);
        i += 1;
    }

    let valid: bool = kani::any();
    let mut words = initial;
    let mask = ValidityMut::over_words(&mut words, count);
    mask.set(row, valid);
    assert_eq!(mask.is_valid(row), valid);
    assert_eq!(words[word] & !bit, initial[word] & !bit);

    let mut words = initial;
    let mut mask = ValidityMut::over_words(&mut words, count);
    mask.set_all_invalid();
    assert!(!mask.is_valid(row));
    let used = count.div_ceil(64);
    let mut i = 0;
    while i < 32 {
        assert_eq!(words[i], if i < used { 0 } else { initial[i] });
        i += 1;
    }
}

/// The two `string_t` forms over every byte string of at most 64 bytes: up to 12 bytes inline (the length, the bytes,
/// zero padding), longer ones carry the length, the first four bytes as the prefix and a pointer at the bytes; both
/// read back as the original through `bytes_as_slice`.
#[kani::proof]
#[kani::unwind(66)]
fn string_t_forms_hold_length_prefix_and_bytes() {
    let buf: [u8; 64] = kani::any();
    let len: usize = kani::any();
    kani::assume(len <= 64);
    let bytes = &buf[..len];
    if len <= DUCKDB_V2_BYTES_INLINE_LENGTH as usize {
        let entry = bytes_inlined(bytes);
        assert_eq!(entry.length() as usize, len);
        // SAFETY: the union was built in its inlined form just above
        let inlined = unsafe { entry.value.inlined.inlined };
        let mut i = 0;
        while i < 12 {
            assert_eq!(inlined[i] as u8, if i < len { buf[i] } else { 0 });
            i += 1;
        }
        // SAFETY: a `string_t` whose bytes live inside it
        assert_eq!(unsafe { bytes_as_slice(&entry) }, bytes);
    } else {
        let entry = bytes_pointer(bytes);
        assert_eq!(entry.length() as usize, len);
        // SAFETY: the union was built in its pointer form just above
        let pointer = unsafe { entry.value.pointer };
        assert_eq!(pointer.prefix.map(|c| c as u8), [buf[0], buf[1], buf[2], buf[3]]);
        assert_eq!(pointer.ptr as *const u8, buf.as_ptr());
        // SAFETY: `buf` outlives the entry pointing into it
        assert_eq!(unsafe { bytes_as_slice(&entry) }, bytes);
    }
}

/// The entry count a view derives for a selection of at most 8 indices below 2048: `count` without a selection, 0
/// for no rows, else one above the largest index, so every selected index is below it and the bound is tight.
#[kani::proof]
#[kani::unwind(10)]
fn derived_data_count_bounds_every_selected_index() {
    let count: idx_t = kani::any();
    assert_eq!(derived_data_count(None, count), count);
    let sel: [duckdb_v2_sel_t; 8] = kani::any();
    let len: usize = kani::any();
    kani::assume(len <= 8);
    let mut i = 0;
    while i < len {
        kani::assume(sel[i] < 2048);
        i += 1;
    }
    let data_count = derived_data_count(Some(&sel[..len]), len as idx_t);
    if len == 0 {
        assert_eq!(data_count, 0);
    } else {
        assert!(data_count >= 1 && data_count <= 2048);
        let mut tight = false;
        let mut i = 0;
        while i < len {
            assert!(idx_t::from(sel[i]) < data_count);
            tight |= idx_t::from(sel[i]) + 1 == data_count;
            i += 1;
        }
        assert!(tight);
    }
}
