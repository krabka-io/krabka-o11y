/// The files of one Prometheus TSDB block directory, as uploaded.
#[derive(Clone, Copy, Debug)]
pub struct TsdbBlockFiles<'a> {
    pub index: &'a [u8],
    /// The `chunks/NNNNNN` segments, sorted by file name.
    pub chunk_segments: &'a [&'a [u8]],
    /// The `tombstones` file. A block without deletions may omit it.
    pub tombstones: Option<&'a [u8]>,
}
