/// Points one range query may memoize across its grid-driven leaves.
///
/// A grid-driven leaf holds `steps x series` results at once, where the per-step
/// path holds one step's worth. At 24 bytes a point the budget is about 50 MB,
/// which covers the shapes a dashboard sends — a thousand series over a day at a
/// one-minute step is 1.4 million points — while keeping a query that pairs the
/// engine's 11,000-point resolution cap with a wide selector from trading the
/// step loop's bounded footprint for an unbounded one. A leaf over the budget
/// falls back to per-step planning, which is what the driver did before.
pub(crate) const MAX_GRID_LEAF_POINTS: usize = 2_000_000;
