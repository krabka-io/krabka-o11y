use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug)]
pub(crate) struct QueryRequestTiming {
    pub(crate) started: Instant,
    pub(crate) queue: Duration,
}
