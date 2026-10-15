use super::{FrontendConfig, TenantId, Time};

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

/// One individually stored profile: its `__profile_id__` label value, and the
/// labels each of its exemplars carries.
#[derive(Clone, Copy)]
pub(crate) struct IndividualProfile<'a> {
    pub(crate) profile_id: &'a str,
    pub(crate) labels: &'a [krabka_pprof::ProfileLabel],
}

/// Not `Eq`: the sharded variant carries a [`FrontendConfig`], whose shard width
/// is a `f64`-backed quantity.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum QueryExecution {
    Direct,
    Sharded(FrontendConfig),
}

/// A query for the exemplars of each series that `target` selects, grouped
/// by the `group_by` labels and bucketed by `step` over `range`.
#[derive(Clone, Copy)]
pub(crate) struct SeriesExemplarQuery<'a> {
    pub(crate) target: QueryTarget<'a>,
    pub(crate) group_by: &'a [String],
    pub(crate) step: Time,
    pub(crate) range: QueryRange,
    /// Keeps only stacks that match these call sites; empty keeps every stack.
    pub(crate) call_sites: &'a [String],
}

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
    pub(crate) range: QueryRange,
}

pub(crate) type SpanExemplarsBySeries = std::collections::BTreeMap<
    super::SeriesKey,
    std::collections::BTreeMap<i64, Vec<super::pb::types::v1::Exemplar>>,
>;

pub(crate) type HeatmapSpanExemplarsBySeries = std::collections::BTreeMap<
    super::SeriesKey,
    std::collections::BTreeMap<i64, Vec<super::pb::querier::v1::Exemplar>>,
>;

/// The slot grid of a heatmap query: `step`-wide slots over `[start, end]`,
/// all in Unix milliseconds.
#[derive(Clone, Copy)]
pub(crate) struct HeatmapSlotsMillis {
    pub(crate) start: i64,
    pub(crate) end: i64,
    pub(crate) step: i64,
}

pub(crate) fn heatmap_slot_timestamp(
    start_ms: i64,
    end_ms: i64,
    step_ms: i64,
    timestamp: i64,
) -> Option<i64> {
    if start_ms >= end_ms
        || step_ms <= 0
        || timestamp < start_ms.saturating_sub(step_ms)
        || timestamp > end_ms
    {
        return None;
    }
    let delta = (i128::from(timestamp) - i128::from(start_ms)).max(0);
    let rounded = i128::from(start_ms)
        + (delta + i128::from(step_ms) - 1) / i128::from(step_ms) * i128::from(step_ms);
    i64::try_from(rounded).ok()
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct QueryTarget<'a> {
    pub tenant: &'a super::TenantId,
    pub profile_type: &'a str,
    pub label_selector: &'a str,
}

impl<'a> QueryTarget<'a> {
    pub(crate) const fn new(
        tenant: &'a super::TenantId,
        profile_type: &'a str,
        label_selector: &'a str,
    ) -> Self {
        Self {
            tenant,
            profile_type,
            label_selector,
        }
    }
}

pub(crate) use krabka_pprof::MillisRange as QueryRange;
pub(crate) type SeriesKey = Vec<krabka_pprof::ProfileLabel>;
