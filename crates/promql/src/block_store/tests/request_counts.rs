/// The object-store requests one stretch of work made, by kind.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct RequestCounts {
    pub(crate) heads: usize,
    pub(crate) range_gets: usize,
    pub(crate) full_gets: usize,
    pub(crate) lists: usize,
}

impl RequestCounts {
    pub(crate) fn per_block(blocks: usize, heads: usize, range_gets: usize) -> Self {
        Self {
            heads: blocks * heads,
            range_gets: blocks * range_gets,
            full_gets: 0,
            lists: 0,
        }
    }

    pub(crate) fn since(self, earlier: Self) -> Self {
        Self {
            heads: self.heads - earlier.heads,
            range_gets: self.range_gets - earlier.range_gets,
            full_gets: self.full_gets - earlier.full_gets,
            lists: self.lists - earlier.lists,
        }
    }
}
