/// Zero-copy view of a native-histogram span list.
#[derive(Clone, Copy, Debug)]
pub struct HistogramSpanView<'a> {
    pub(crate) offsets: &'a [i32],
    pub(crate) lengths: &'a [u32],
}

impl<'a> HistogramSpanView<'a> {
    #[must_use]
    pub fn len(&self) -> usize {
        self.offsets.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }

    #[must_use]
    pub fn offsets(&self) -> &'a [i32] {
        self.offsets
    }

    #[must_use]
    pub fn lengths(&self) -> &'a [u32] {
        self.lengths
    }
}
