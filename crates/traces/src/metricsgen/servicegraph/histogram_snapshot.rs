
#[derive(Clone, Copy)]
pub(crate) struct HistogramSnapshot<'a> {
    pub(crate) sum: f64,
    pub(crate) count: f64,
    pub(crate) bucket_edges_ns: &'a [f64],
    pub(crate) bucket_counts: &'a [u64],
}
