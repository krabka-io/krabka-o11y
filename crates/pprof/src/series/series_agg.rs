/// Select-series aggregation mode. The bodies come in the next slice.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SeriesAgg {
    Sum,
    Average,
}
