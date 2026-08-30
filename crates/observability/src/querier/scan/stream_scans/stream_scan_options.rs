use super::*;

#[derive(Clone, Copy)]
pub(crate) struct StreamScanOptions {
    pub(crate) direction: LokiDirection,
    pub(crate) limit: Option<usize>,
    pub(crate) end_exclusive: Option<i64>,
    pub(crate) allow_limit_short_circuit: bool,
    pub(crate) block_fetch_concurrency: NonZeroUsize,
}

impl StreamScanOptions {
    pub(crate) fn exhaustive() -> Self {
        Self {
            direction: LokiDirection::Forward,
            limit: None,
            end_exclusive: None,
            allow_limit_short_circuit: false,
            block_fetch_concurrency: NonZeroUsize::new(8)
                .expect("default block fetch concurrency is nonzero"),
        }
    }

    pub(crate) fn from_stream_options(
        direction: LokiDirection,
        limit: Option<usize>,
        interval: Option<i64>,
        end_exclusive: Option<i64>,
    ) -> Self {
        Self {
            direction,
            limit,
            end_exclusive,
            allow_limit_short_circuit: limit.is_some() && interval.is_none(),
            block_fetch_concurrency: NonZeroUsize::new(8)
                .expect("default block fetch concurrency is nonzero"),
        }
    }

    pub(crate) fn with_block_fetch_concurrency(mut self, concurrency: NonZeroUsize) -> Self {
        self.block_fetch_concurrency = concurrency;
        self
    }

    pub(crate) fn reached_limit(self, streams: &BTreeMap<Labels, Vec<[String; 2]>>) -> bool {
        self.allow_limit_short_circuit
            && self
                .limit
                .is_some_and(|limit| count_stream_map_lines(streams, self.end_exclusive) >= limit)
    }

    pub(crate) fn block_fetch_concurrency(self) -> usize {
        if !self.allow_limit_short_circuit {
            return self.block_fetch_concurrency.get();
        }
        self.limit
            .map_or(self.block_fetch_concurrency.get(), |limit| {
                self.block_fetch_concurrency.get().min(limit.max(1))
            })
    }
}
