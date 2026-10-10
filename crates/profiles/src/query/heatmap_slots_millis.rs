/// The slot grid of a heatmap query: `step`-wide slots over `[start, end]`,
/// all in Unix milliseconds.
#[derive(Clone, Copy)]
pub(crate) struct HeatmapSlotsMillis {
    pub(crate) start: i64,
    pub(crate) end: i64,
    pub(crate) step: i64,
}
