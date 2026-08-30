use super::*;

pub(crate) fn append_matching_log_row(
    streams: &mut BTreeMap<Labels, Vec<[String; 2]>>,
    plan: &StreamPlan,
    label_index: &LabelIndex,
    row: QueryRow<'_>,
    delete_filters: &[ActiveLogDeleteFilter],
) -> Result<(), QueryError> {
    let QueryRow {
        fingerprint,
        timestamp_ns,
        line,
        structured_metadata,
    } = row;
    if timestamp_ns < plan.time_range.start_ns
        || timestamp_ns > plan.time_range.end_ns
        || !plan.fingerprints.contains(&fingerprint)
    {
        return Ok(());
    }

    let labels = label_index.labels_for(&plan.tenant, fingerprint).ok_or(
        QueryError::MissingSeriesLabels {
            tenant: plan.tenant.clone(),
            fingerprint,
        },
    )?;
    if is_deleted_log_entry(
        delete_filters,
        labels,
        line,
        structured_metadata,
        timestamp_ns,
    ) {
        return Ok(());
    }
    if let Some((stream_labels, current_line)) =
        matching_loki_stream_entry(&plan.query, labels, line, structured_metadata, timestamp_ns)
    {
        streams
            .entry(stream_labels)
            .or_default()
            .push([timestamp_ns.to_string(), current_line]);
    }

    Ok(())
}
