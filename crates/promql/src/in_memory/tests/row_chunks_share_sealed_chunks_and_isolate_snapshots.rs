use std::sync::Arc;

use assert2::check;

use crate::in_memory::{ROW_CHUNK_LEN, RowChunks};

/// Fills `chunks` with `0..count`, so a row's value is also its position.
fn filled(count: usize) -> RowChunks<usize> {
    let mut chunks = RowChunks::default();
    for row in 0..count {
        chunks.push(row);
    }
    chunks
}

/// Rows come back in insertion order across the sealed/open boundary.
///
/// The boundary is the whole point of the type, and it is invisible from the
/// outside, so a test that stays under [`ROW_CHUNK_LEN`] would never cross it.
#[test]
pub(crate) fn rows_read_back_in_insertion_order_across_sealed_chunks() {
    let chunks = filled(ROW_CHUNK_LEN * 2 + 7);

    check!(chunks.len() == ROW_CHUNK_LEN * 2 + 7);
    check!(!chunks.is_empty());
    check!(chunks.iter().copied().eq(0..ROW_CHUNK_LEN * 2 + 7));
}

/// A clone shares its sealed chunks by pointer rather than copying them.
///
/// This is the claim the type exists to make -- a copy-on-write clone of the
/// head stops scaling with the head's size -- and it is not observable from
/// behaviour alone, so it is pinned by pointer identity.
#[test]
pub(crate) fn a_clone_shares_sealed_chunks_rather_than_copying_them() {
    let original = filled(ROW_CHUNK_LEN * 2);
    let clone = original.clone();

    let shared = original
        .sealed_chunks()
        .zip(clone.sealed_chunks())
        .all(|(left, right)| Arc::ptr_eq(left, right));

    check!(original.sealed_chunks().count() == 2);
    check!(shared, "a clone must share every sealed chunk by pointer");
}

/// `retain` rebuilds only the chunks it changes, and leaves a snapshot alone.
///
/// A snapshot taken before the retain must still return every row the retain
/// dropped: rewriting a sealed chunk in place would corrupt a scan already
/// reading it, which is the failure the rebuild-and-swap exists to prevent.
#[test]
pub(crate) fn retain_rebuilds_only_what_it_changes_and_leaves_a_snapshot_intact() {
    let mut chunks = filled(ROW_CHUNK_LEN * 3);
    let snapshot = chunks.clone();
    let untouched = Arc::clone(chunks.sealed_chunks().next().expect("a first sealed chunk"));

    // Drop one row from the second chunk and nothing from the first.
    chunks.retain(|row| *row != ROW_CHUNK_LEN + 1);

    check!(chunks.len() == ROW_CHUNK_LEN * 3 - 1);
    check!(!chunks.iter().any(|row| *row == ROW_CHUNK_LEN + 1));
    check!(
        Arc::ptr_eq(
            chunks.sealed_chunks().next().expect("a first sealed chunk"),
            &untouched,
        ),
        "a chunk that loses no rows must be carried over by pointer, not rebuilt"
    );
    check!(
        snapshot.len() == ROW_CHUNK_LEN * 3,
        "a snapshot taken before the retain keeps every row it dropped"
    );
    check!(snapshot.iter().copied().eq(0..ROW_CHUNK_LEN * 3));
}

/// A chunk that loses every row is dropped rather than kept empty.
#[test]
pub(crate) fn retain_drops_a_chunk_that_keeps_nothing() {
    let mut chunks = filled(ROW_CHUNK_LEN * 2);

    // Keep only the second sealed chunk's rows.
    chunks.retain(|row| *row >= ROW_CHUNK_LEN);

    check!(chunks.len() == ROW_CHUNK_LEN);
    check!(chunks.sealed_chunks().count() == 1);
    check!(chunks.iter().copied().eq(ROW_CHUNK_LEN..ROW_CHUNK_LEN * 2));
}

/// Retaining nothing empties the storage without leaving stale chunks behind.
#[test]
pub(crate) fn retain_that_keeps_nothing_leaves_the_storage_empty() {
    let mut chunks = filled(ROW_CHUNK_LEN + 5);

    chunks.retain(|_| false);

    check!(chunks.is_empty());
    check!(chunks.len() == 0);
    check!(chunks.sealed_chunks().count() == 0);
    check!(chunks.iter().next().is_none());
}
