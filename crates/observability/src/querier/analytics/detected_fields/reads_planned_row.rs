use super::{QuerierState, QueryError, StreamPlan, is_deleted_log_entry};
use crate::{ActiveLogDeleteFilter, LogRow};

/// Whether a planned read's range includes a row at its end.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum RangeEnd {
    Inclusive,
    Exclusive,
}

/// An analytics read under `plan`, with the delete requests that remove rows
/// from it.
pub(crate) struct PlannedRead<'a> {
    pub(crate) state: &'a QuerierState,
    pub(crate) plan: &'a StreamPlan,
    pub(crate) delete_filters: &'a [ActiveLogDeleteFilter],
    pub(crate) end: RangeEnd,
}

/// Whether `read` reads a stored `row`: the row is in the plan's series and
/// time range, no delete request removes it, and it matches the plan's query.
pub(crate) fn reads_planned_row(read: &PlannedRead<'_>, row: &LogRow) -> Result<bool, QueryError> {
    let PlannedRead {
        state,
        plan,
        delete_filters,
        end,
    } = *read;
    let past_end = match end {
        RangeEnd::Exclusive => row.timestamp_ns >= plan.time_range.end_ns,
        RangeEnd::Inclusive => row.timestamp_ns > plan.time_range.end_ns,
    };
    if !plan.fingerprints.contains(&row.series_fingerprint)
        || row.timestamp_ns < plan.time_range.start_ns
        || past_end
    {
        return Ok(false);
    }
    let tenant = plan.tenant.as_str();
    let labels = state
        .label_index
        .labels_for(tenant, row.series_fingerprint)
        .ok_or(QueryError::MissingSeriesLabels {
            tenant: tenant.to_string(),
            fingerprint: row.series_fingerprint,
        })?;
    if is_deleted_log_entry(
        delete_filters,
        labels,
        &row.line,
        &row.structured_metadata,
        row.timestamp_ns,
    ) {
        return Ok(false);
    }
    Ok(plan
        .query
        .matches_with_fields(labels, &row.line, &row.structured_metadata))
}
