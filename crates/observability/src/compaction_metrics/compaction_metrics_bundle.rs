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
    deletion_sweeps: Counter,
    deleted_blocks: Counter,
    deleted_sidecars: Counter,
    deletion_failures: Counter,
    orphan_sweeps: Counter,
    orphans_deleted: Counter,
    orphan_failures: Counter,
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
        for (name, help, counter) in [
            (
                "deletion_sweeps",
                "Block deletion passes run.",
                &this.deletion_sweeps,
            ),
            (
                "deleted_blocks",
                "Data blocks deleted.",
                &this.deleted_blocks,
            ),
            (
                "deleted_sidecars",
                "Block sidecars deleted.",
                &this.deleted_sidecars,
            ),
            (
                "deletion_failures",
                "Block objects that could not be deleted.",
                &this.deletion_failures,
            ),
            (
                "orphan_sweeps",
                "Orphan reconciliation passes run.",
                &this.orphan_sweeps,
            ),
            (
                "orphans_deleted",
                "Orphaned block objects deleted.",
                &this.orphans_deleted,
            ),
            (
                "orphan_failures",
                "Orphaned block objects that could not be deleted.",
                &this.orphan_failures,
            ),
        ] {
            registry.register(name, help, counter.clone());
        }

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
            deletion_sweeps: Counter::default(),
            deleted_blocks: Counter::default(),
            deleted_sidecars: Counter::default(),
            deletion_failures: Counter::default(),
            orphan_sweeps: Counter::default(),
            orphans_deleted: Counter::default(),
            orphan_failures: Counter::default(),
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

    /// Records one block-deletion pass and its result.
    pub fn record_deleted(&self, blocks: u64, sidecars: u64, failures: u64) {
        self.deletion_sweeps.inc();
        self.deleted_blocks.inc_by(blocks);
        self.deleted_sidecars.inc_by(sidecars);
        self.deletion_failures.inc_by(failures);
    }

    /// Records one orphan-reconciliation pass and its result.
    pub fn record_orphan_sweep(&self, deleted: u64, failures: u64) {
        self.orphan_sweeps.inc();
        self.orphans_deleted.inc_by(deleted);
        self.orphan_failures.inc_by(failures);
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
