use super::BlockLevel;

/// The fan-in cap a policy falls back to.
pub const DEFAULT_MAX_BLOCKS_PER_JOB: usize = 8;
/// The row target a policy falls back to: a block this large is left alone.
pub const DEFAULT_TARGET_ROWS_PER_BLOCK: usize = 1_000_000;
/// The ladder height a policy falls back to.
pub const DEFAULT_MAX_LEVEL: BlockLevel = BlockLevel(4);
/// The level-zero grouping window a policy falls back to: two hours.
pub const DEFAULT_LEVEL_WINDOW_NS: i64 = 7_200_000_000_000;

/// What a compaction planner is allowed to do.
///
/// The three caps are what make planning terminate. A job needs at least two
/// inputs and takes at most `max_blocks_per_job`, so it strictly reduces the
/// block count. Its inputs must sit below `max_level`, so a block is rewritten
/// at most `max_level` times. And a block that has already reached
/// `target_rows_per_block` is never an input, so the bytes that cost the most
/// to move are moved once and then left alone.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CompactionPolicy {
    max_blocks_per_job: usize,
    target_rows_per_block: usize,
    max_level: BlockLevel,
    level_window_ns: i64,
}

impl CompactionPolicy {
    /// Builds a policy, clamping each cap into the range that keeps planning
    /// finite: a job of fewer than two blocks is a rewrite, a target of zero
    /// rows would seal every block on sight, a ladder of zero levels would
    /// compact nothing, and a window of zero nanoseconds has no buckets.
    #[must_use]
    pub fn new(
        max_blocks_per_job: usize,
        target_rows_per_block: usize,
        max_level: BlockLevel,
        level_window_ns: i64,
    ) -> Self {
        Self {
            max_blocks_per_job: max_blocks_per_job.max(2),
            target_rows_per_block: target_rows_per_block.max(1),
            max_level: BlockLevel(max_level.get().max(1)),
            level_window_ns: level_window_ns.max(1),
        }
    }

    #[must_use]
    pub const fn max_blocks_per_job(self) -> usize {
        self.max_blocks_per_job
    }

    #[must_use]
    pub const fn target_rows_per_block(self) -> usize {
        self.target_rows_per_block
    }

    #[must_use]
    pub const fn max_level(self) -> BlockLevel {
        self.max_level
    }

    /// The time window blocks at `level` are bucketed into.
    ///
    /// The window doubles with each level, so the ladder widens as it climbs:
    /// level zero merges blocks that share a two-hour bucket by default, level
    /// one a four-hour bucket, and so on. Without the widening a level-one
    /// block would keep meeting its neighbours in the same narrow bucket and
    /// the ladder would buy nothing.
    #[must_use]
    pub fn window_ns_for(self, level: BlockLevel) -> i64 {
        let factor = 1_i64
            .checked_shl(level.get())
            .filter(|factor| *factor > 0)
            .unwrap_or(i64::MAX);
        self.level_window_ns.saturating_mul(factor)
    }
}

impl Default for CompactionPolicy {
    fn default() -> Self {
        Self::new(
            DEFAULT_MAX_BLOCKS_PER_JOB,
            DEFAULT_TARGET_ROWS_PER_BLOCK,
            DEFAULT_MAX_LEVEL,
            DEFAULT_LEVEL_WINDOW_NS,
        )
    }
}
