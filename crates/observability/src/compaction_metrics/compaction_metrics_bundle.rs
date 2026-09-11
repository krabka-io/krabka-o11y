use super::{CompactionStatusLabel, Counter, Family, Histogram, Registry, Time, TimeExt};

/// Duration buckets for one compaction pass, in seconds.
///
/// A pass that finds nothing to do returns in milliseconds. A pass that merges
/// a full job of blocks reads, sorts and re-encodes them, and that runs for
/// minutes. The range covers both, and its top bucket is where a pass has
/// stopped being a background job.
const COMPACTION_DURATION_BUCKETS: [f64; 11] = [
    0.01, 0.1, 0.5, 1.0, 5.0, 15.0, 60.0, 180.0, 600.0, 1800.0, 3600.0,
];

/// The compaction instruments, as a cheaply-clonable bundle of handles.
#[derive(Clone, Debug)]
pub struct CompactionMetrics {
    runs: Family<CompactionStatusLabel, Counter>,
    duration: Histogram,
    blocks: Counter,
}

impl CompactionMetrics {
    /// Registers the instruments into a `compaction` sub-registry of
    /// `registry`, and returns the handles.
    pub fn register(registry: &mut Registry) -> Self {
        let this = Self::unregistered();
        let registry = registry.sub_registry_with_prefix("compaction");

        registry.register(
            "runs",
            "Compaction passes this role has finished, by outcome. A pass \
             that found nothing to do counts as `ok`.",
            this.runs.clone(),
        );
        registry.register(
            "duration_seconds",
            "How long one compaction pass took, in seconds. A pass that \
             failed is observed here too, because the time it spent is time \
             the compactor was busy.",
            this.duration.clone(),
        );
        registry.register(
            "blocks",
            "Blocks compaction has written. This counts the output, so it \
             falls as compaction does its job and the input shrinks. The byte \
             volume behind it is in objstore_operation_transferred_bytes_total, \
             which is where the writes actually happen.",
            this.blocks.clone(),
        );

        this
    }

    /// Handles that no registry holds, so nothing they record is exported.
    ///
    /// This is for a call site that has no registry, which in practice means a
    /// test. Do not use it in a service.
    #[must_use]
    pub fn unregistered() -> Self {
        Self {
            runs: Family::default(),
            duration: Histogram::new(COMPACTION_DURATION_BUCKETS),
            blocks: Counter::default(),
        }
    }

    /// Records one finished compaction pass.
    ///
    /// Call this on both arms of the pass, so a compactor that is failing
    /// every pass is told apart from one that has nothing to do. A pass that
    /// planned no work is still a pass, and counts as `ok`.
    pub fn record_run(&self, ok: bool, elapsed: Time) {
        self.runs
            .get_or_create(&CompactionStatusLabel::for_outcome(ok))
            .inc();
        self.duration.observe(elapsed.secs_f64());
    }

    /// Records the `blocks` a pass wrote.
    ///
    /// Call this once per pass with the total, not once per block. A zero
    /// leaves the counter where it was, so a pass that planned nothing is
    /// visible in the run counter and not here.
    pub fn record_output(&self, blocks: u64) {
        if blocks > 0 {
            self.blocks.inc_by(blocks);
        }
    }

    /// The pass count for one outcome.
    #[must_use]
    pub fn runs(&self, ok: bool) -> u64 {
        self.runs
            .get_or_create(&CompactionStatusLabel::for_outcome(ok))
            .get()
    }

    /// The written-block total.
    #[must_use]
    pub fn blocks(&self) -> u64 {
        self.blocks.get()
    }
}
