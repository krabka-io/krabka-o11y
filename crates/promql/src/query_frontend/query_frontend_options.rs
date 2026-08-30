use super::Time;

/// Query-frontend range splitting and sharding options.
///
/// Not `Eq`: `split_interval` is a [`Time`], which stores `f64`.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct QueryFrontendOptions {
    /// Width of the absolute window that each sub-range is split on.
    pub split_interval: Time,
    pub shard_count: usize,
}
