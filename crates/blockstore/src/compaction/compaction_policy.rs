use super::{BlockLevel, BlockTimestampUnit, Time, hours};

/// The fan-in cap a policy falls back to.
pub const DEFAULT_MAX_BLOCKS_PER_JOB: usize = 8;
/// The row target a policy falls back to: a block this large is left alone.
pub const DEFAULT_TARGET_ROWS_PER_BLOCK: usize = 1_000_000;
/// The ladder height a policy falls back to.
pub const DEFAULT_MAX_LEVEL: BlockLevel = BlockLevel(4);
/// The level-zero grouping window a policy falls back to.
pub const DEFAULT_LEVEL_WINDOW: Time = hours(2);

/// What a compaction planner is allowed to do.
///
/// The three caps are what make planning terminate. A job needs at least two
/// inputs and takes at most `max_blocks_per_job`, so it strictly reduces the
/// block count. Its inputs must sit below `max_level`, so a block is rewritten
/// at most `max_level` times. And a block that has already reached
/// `target_rows_per_block` is never an input, so the bytes that cost the most
/// to move are moved once and then left alone.
///
/// # The window and its unit
///
/// The grouping window is a wall-clock extent, so it is a [`Time`]. The block
/// timestamps it is compared against are plain `i64` ticks, and the signals do
/// not agree on what a tick is: metrics and profiles count epoch milliseconds,
/// traces counts epoch nanoseconds. A policy therefore carries the unit its
/// blocks count in, and converts the window itself in
/// [`Self::window_ticks_for`].
///
/// Nothing here can be got right by a caller that passes a bare integer. A
/// millisecond-stamped index measured with a nanosecond window puts every
/// block it holds into one bucket, so the rule that a job never spans two
/// windows stops restricting anything, and it does so silently.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct CompactionPolicy {
    max_blocks_per_job: usize,
    target_rows_per_block: usize,
    max_level: BlockLevel,
    level_window: Time,
    timestamp_unit: BlockTimestampUnit,
}

impl CompactionPolicy {
    /// Builds a policy, clamping each cap into the range that keeps planning
    /// finite: a job of fewer than two blocks is a rewrite, a target of zero
    /// rows would seal every block on sight, and a ladder of zero levels would
    /// compact nothing.
    ///
    /// `level_window` is the level-zero grouping window, and `timestamp_unit`
    /// is the unit the blocks this policy plans count their timestamps in.
    #[must_use]
    pub fn new(
        max_blocks_per_job: usize,
        target_rows_per_block: usize,
        max_level: BlockLevel,
        level_window: Time,
        timestamp_unit: BlockTimestampUnit,
    ) -> Self {
        Self {
            max_blocks_per_job: max_blocks_per_job.max(2),
            target_rows_per_block: target_rows_per_block.max(1),
            max_level: BlockLevel(max_level.get().max(1)),
            level_window,
            timestamp_unit,
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

    /// The level-zero grouping window, as the caller configured it.
    #[must_use]
    pub const fn level_window(self) -> Time {
        self.level_window
    }

    /// The unit the blocks this policy plans count their timestamps in.
    #[must_use]
    pub const fn timestamp_unit(self) -> BlockTimestampUnit {
        self.timestamp_unit
    }

    /// The window blocks at `level` are bucketed into, counted in the unit the
    /// block timestamps use.
    ///
    /// The window doubles with each level, so the ladder widens as it climbs:
    /// level zero merges blocks that share a two-hour bucket by default, level
    /// one a four-hour bucket, and so on. Without the widening a level-one
    /// block would keep meeting its neighbours in the same narrow bucket and
    /// the ladder would buy nothing.
    ///
    /// The result is at least one tick. A window of zero ticks would divide by
    /// zero where the planner buckets a block, and a window a caller left
    /// negative is not a window at all.
    #[must_use]
    pub fn window_ticks_for(self, level: BlockLevel) -> i64 {
        let factor = 1_i64
            .checked_shl(level.get())
            .filter(|factor| *factor > 0)
            .unwrap_or(i64::MAX);
        self.timestamp_unit
            .ticks(self.level_window)
            .max(1)
            .saturating_mul(factor)
    }
}
