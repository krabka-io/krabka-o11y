use std::sync::Arc;

use super::ROW_CHUNK_LEN;

/// Append-mostly row storage a copy-on-write clone shares instead of copying.
///
/// New rows land in `open`. Once `open` holds [`ROW_CHUNK_LEN`] of them it is
/// frozen into an `Arc<[R]>` and moved to `sealed`, and from then on every
/// clone of the store shares that chunk by pointer. Cloning a `RowChunks`
/// therefore copies at most `ROW_CHUNK_LEN` rows and bumps two refcounts,
/// whatever the total row count: the cost of a write that has to clone stops
/// scaling with the size of the head.
///
/// A sealed chunk is immutable. [`RowChunks::retain`] never writes through an
/// existing `Arc<[R]>`; it builds a replacement chunk and swaps the pointer.
/// A snapshot taken before a retain keeps the chunks it captured, so it cannot
/// observe a chunk that is halfway through being rewritten.
pub(crate) struct RowChunks<R> {
    /// Frozen chunks, oldest first. Behind one `Arc` so that appending a chunk
    /// -- which happens once per `ROW_CHUNK_LEN` rows -- is the only write that
    /// has to copy the chunk list.
    sealed: Arc<Vec<Arc<[R]>>>,
    /// The chunk still being filled, and the only rows a clone copies.
    open: Vec<R>,
    /// Rows across `sealed` and `open`, kept so `len` does not walk the chunks.
    len: usize,
}

impl<R> RowChunks<R> {
    /// Rows held across every chunk.
    pub(crate) fn len(&self) -> usize {
        self.len
    }

    /// Whether the storage holds no rows at all.
    pub(crate) fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// The frozen chunks, oldest first.
    ///
    /// Exposed so a test can assert that a clone or a [`RowChunks::retain`]
    /// shares a chunk by pointer rather than copying it. That is the property
    /// this type exists for, and it is invisible from the rows alone -- and it
    /// is why this is a test-only seam: nothing in the running engine needs to
    /// look at a chunk as a chunk.
    #[cfg(test)]
    pub(crate) fn sealed_chunks(&self) -> impl Iterator<Item = &Arc<[R]>> {
        self.sealed.iter()
    }

    /// Every row in insertion order.
    pub(crate) fn iter(&self) -> impl Iterator<Item = &R> {
        self.sealed
            .iter()
            .flat_map(|chunk| chunk.iter())
            .chain(self.open.iter())
    }

    /// Appends `row`, sealing the open chunk once it is full.
    pub(crate) fn push(&mut self, row: R) {
        self.open.push(row);
        self.len += 1;
        if self.open.len() >= ROW_CHUNK_LEN {
            let sealed = Arc::from(std::mem::take(&mut self.open));
            Arc::make_mut(&mut self.sealed).push(sealed);
        }
    }
}

impl<R: Clone> RowChunks<R> {
    /// Drops every row for which `keep` is false.
    ///
    /// A chunk whose rows all survive is carried over by pointer, so a retain
    /// that evicts nothing -- the common case between two retention horizons --
    /// copies no rows at all. A chunk that loses rows is rebuilt into a fresh
    /// `Arc<[R]>` rather than edited, which is what keeps an outstanding
    /// snapshot's view of it intact.
    pub(crate) fn retain(&mut self, keep: impl Fn(&R) -> bool) {
        let rebuilt = self
            .sealed
            .iter()
            .filter_map(|chunk| {
                if chunk.iter().all(&keep) {
                    return Some(Arc::clone(chunk));
                }
                let survivors: Vec<R> = chunk.iter().filter(|row| keep(row)).cloned().collect();
                (!survivors.is_empty()).then(|| Arc::from(survivors))
            })
            .collect::<Vec<Arc<[R]>>>();
        self.open.retain(&keep);
        self.len = rebuilt.iter().map(|chunk| chunk.len()).sum::<usize>() + self.open.len();
        self.sealed = Arc::new(rebuilt);
    }
}

impl<R> Clone for RowChunks<R>
where
    R: Clone,
{
    fn clone(&self) -> Self {
        Self {
            sealed: Arc::clone(&self.sealed),
            open: self.open.clone(),
            len: self.len,
        }
    }
}

impl<R> Default for RowChunks<R> {
    fn default() -> Self {
        Self {
            sealed: Arc::new(Vec::new()),
            open: Vec::new(),
            len: 0,
        }
    }
}
