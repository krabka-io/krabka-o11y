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

struct CompactionCounterRegistration<'a> {
    name: &'static str,
    help: &'static str,
    counter: &'a Counter,
}

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
        for registration in [
            CompactionCounterRegistration {
                name: "deletion_sweeps",
                help: "Block deletion passes run.",
                counter: &this.deletion_sweeps,
            },
            CompactionCounterRegistration {
                name: "deleted_blocks",
                help: "Data blocks deleted.",
                counter: &this.deleted_blocks,
            },
            CompactionCounterRegistration {
                name: "deleted_sidecars",
                help: "Block sidecars deleted.",
                counter: &this.deleted_sidecars,
            },
            CompactionCounterRegistration {
                name: "deletion_failures",
                help: "Block objects that could not be deleted.",
                counter: &this.deletion_failures,
            },
            CompactionCounterRegistration {
                name: "orphan_sweeps",
                help: "Orphan reconciliation passes run.",
                counter: &this.orphan_sweeps,
            },
            CompactionCounterRegistration {
                name: "orphans_deleted",
                help: "Orphaned block objects deleted.",
                counter: &this.orphans_deleted,
            },
            CompactionCounterRegistration {
                name: "orphan_failures",
                help: "Orphaned block objects that could not be deleted.",
                counter: &this.orphan_failures,
            },
        ] {
            registry.register(
                registration.name,
                registration.help,
                registration.counter.clone(),
            );
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

    /// Runs one compaction pass and records it with
    /// [`record_run`](Self::record_run), whichever way it ends.
    ///
    /// # Errors
    ///
    /// Returns the pass's own error, after recording it as a failed run.
    pub async fn time_run<Pass, Output, PassError>(&self, pass: Pass) -> Result<Output, PassError>
    where
        Pass: Future<Output = Result<Output, PassError>>,
    {
        let started = std::time::Instant::now();
        let outcome = pass.await;
        self.record_run(outcome.is_ok(), Time::from_std(started.elapsed()));
        outcome
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
