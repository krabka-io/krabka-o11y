#[derive(Clone, Debug, Default)]
pub(crate) struct EdgeAgg {
    pub(crate) requests: f64,
    pub(crate) failed: f64,
    pub(crate) client_seconds_sum: f64,
    pub(crate) client_seconds_count: f64,
    pub(crate) client_bucket_counts: Vec<u64>,
    pub(crate) server_seconds_sum: f64,
    pub(crate) server_seconds_count: f64,
    pub(crate) server_bucket_counts: Vec<u64>,
    pub(crate) messaging_seconds_sum: f64,
    pub(crate) messaging_seconds_count: f64,
    pub(crate) messaging_bucket_counts: Vec<u64>,
}

impl EdgeAgg {
    pub(crate) fn new(bucket_count: usize) -> Self {
        Self {
            client_bucket_counts: vec![0; bucket_count],
            server_bucket_counts: vec![0; bucket_count],
            messaging_bucket_counts: vec![0; bucket_count],
            ..Self::default()
        }
    }
}
