use super::{HeatmapSlotsMillis, TenantId};

/// One heatmap exemplar query, as
/// [`QuerierState::select_heatmap_span_exemplars`](super::QuerierState::select_heatmap_span_exemplars)
/// and its individual-profile counterpart take it: the series of `tenant` that
/// `label_selector` matches, read for `profile_type` across `slots`.
#[derive(Clone, Copy)]
pub(crate) struct HeatmapExemplarRequest<'a> {
    pub(crate) tenant: &'a TenantId,
    pub(crate) profile_type: &'a str,
    pub(crate) label_selector: &'a str,
    /// Labels whose values split the exemplars into one series per group.
    pub(crate) group_by: &'a [String],
    pub(crate) slots: HeatmapSlotsMillis,
}
