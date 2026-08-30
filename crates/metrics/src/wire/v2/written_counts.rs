/// Written sample tallies for the v2 response headers.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct WrittenCounts {
    pub samples: u64,
    pub histograms: u64,
    pub exemplars: u64,
}
