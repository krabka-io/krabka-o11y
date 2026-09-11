use super::{Counter, Registry};

/// Produce-side WAL instruments, shared by the four signals.
///
/// [`WalConsumerMetrics`](crate::wal_consumer_metrics::WalConsumerMetrics) says
/// whether the read side of the WAL still moves. This bundle says whether the
/// write side landed what it was given.
///
/// Each signal already counts every failed append under
/// `wal_append_failures_total`. That counter cannot separate the two failures
/// that matter differently to an operator. A request that appended nothing
/// fails cleanly: the client retries, and the retry writes each record one
/// time. A request that appended part of its records and then failed leaves
/// that part durable, and the retry writes it a second time. Only the logs and
/// the profiles read paths have no query-time deduplication, so on those two
/// signals the second copy is permanent.
///
/// So both families here count only the partial case:
///
/// - `wal_partial_batch_appends_total` counts the requests. Watch it for the
///   event.
/// - `wal_unappended_records_total` counts the records a failed batch did not
///   get an ack for. Subtract it from the request's record count to size what
///   a retry rewrites.
///
/// # Cardinality
///
/// Neither family carries a label. A tenant is already counted once per
/// request at the ingest handler, and this is the write path's hottest loop.
#[derive(Clone, Debug)]
pub struct WalProduceMetrics {
    partial_batch_appends: Counter,
    unappended_records: Counter,
}

impl WalProduceMetrics {
    /// Registers both families on `registry` and returns the bundle.
    pub fn register(registry: &mut Registry) -> Self {
        let partial_batch_appends = Counter::default();
        let unappended_records = Counter::default();
        registry.register(
            "wal_partial_batch_appends",
            "WAL batches that appended some, but not all, of one request's records.",
            partial_batch_appends.clone(),
        );
        registry.register(
            "wal_unappended_records",
            "Records of a failed WAL batch that the broker did not ack.",
            unappended_records.clone(),
        );
        Self {
            partial_batch_appends,
            unappended_records,
        }
    }

    /// Records one failed batch of `total` records, of which `appended` acked.
    ///
    /// A batch that appended nothing moves only the record family, because
    /// nothing about it is partial.
    pub fn record_batch_failure(&self, appended: usize, total: usize) {
        if appended > 0 {
            self.partial_batch_appends.inc();
        }
        self.unappended_records
            .inc_by(total.saturating_sub(appended) as u64);
    }

    /// Records a batch that was cancelled before its outcome was known, such
    /// as a WAL append that hit the request timeout.
    ///
    /// The producer may still deliver the records it holds, so this counts as
    /// a partial batch. An operator has to assume that a retry rewrites part
    /// of it.
    pub fn record_batch_abandoned(&self, total: usize) {
        self.partial_batch_appends.inc();
        self.unappended_records.inc_by(total as u64);
    }

    /// Returns the count of partial batches.
    #[must_use]
    pub fn partial_batch_appends(&self) -> u64 {
        self.partial_batch_appends.get()
    }

    /// Returns the count of records a failed batch did not get an ack for.
    #[must_use]
    pub fn unappended_records(&self) -> u64 {
        self.unappended_records.get()
    }
}
