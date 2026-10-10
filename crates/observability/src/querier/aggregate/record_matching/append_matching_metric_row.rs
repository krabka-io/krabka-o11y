use super::{
    LabelIndex, MatchedEntry, MetricSamples, MetricWindow, QueryError, QueryRow, StreamPlan,
    is_deleted_log_entry, matching_loki_metric_sample, record_matching_metric_sample,
};

pub(crate) fn append_matching_metric_row(
    samples: &mut MetricSamples,
    plan: &StreamPlan,
    label_index: &LabelIndex,
    row: QueryRow<'_>,
    window: MetricWindow<'_>,
) -> Result<(), QueryError> {
    let MetricWindow {
        query,
        delete_filters,
        ..
    } = window;
    if !plan.fingerprints.contains(&row.fingerprint) {
        return Ok(());
    }

    let labels = label_index
        .labels_for(&plan.tenant, row.fingerprint)
        .ok_or(QueryError::MissingSeriesLabels {
            tenant: plan.tenant.clone(),
            fingerprint: row.fingerprint,
        })?;
    if is_deleted_log_entry(
        delete_filters,
        labels,
        row.line,
        row.structured_metadata,
        row.timestamp_ns,
    ) {
        return Ok(());
    }
    if let Some((metric_labels, line, unwrap_sample)) = matching_loki_metric_sample(
        query,
        labels,
        row.line,
        row.structured_metadata,
        row.timestamp_ns,
    )? {
        record_matching_metric_sample(
            samples,
            window,
            MatchedEntry {
                timestamp_ns: row.timestamp_ns,
                metric_labels,
                line,
                unwrap_sample,
            },
        );
    }

    Ok(())
}
