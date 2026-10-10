use krabka_pprof::MillisRange;

use super::TenantId;

/// One span-heatmap query, as
/// [`QuerierState::select_span_heatmap_points`](super::QuerierState::select_span_heatmap_points)
/// takes it.
#[derive(Clone, Copy)]
pub(crate) struct SpanHeatmapRequest<'a> {
    pub(crate) tenant: &'a TenantId,
    pub(crate) profile_type: &'a str,
    pub(crate) label_selector: &'a str,
    /// Labels whose values split the heatmap into one series per group.
    pub(crate) group_by: &'a [String],
    pub(crate) range: MillisRange,
}
