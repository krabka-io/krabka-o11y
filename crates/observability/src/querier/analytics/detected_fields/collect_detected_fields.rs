use super::{
    BTreeMap, DetectedFieldStats, DetectedFieldsParams, HeaderMap, HttpQueryError, QuerierState,
    RangeEnd, RequestSecurity, TenantErrorSurface, VolumeRangeLimit, detect_entry_fields,
    is_deleted_log_entry, plan_hot_tail_records, read_planned_log_block, reads_planned_row,
};

pub(crate) async fn collect_detected_fields(
    state: &QuerierState,
    security: &RequestSecurity,
    headers: &HeaderMap,
    params: &DetectedFieldsParams,
) -> Result<BTreeMap<String, DetectedFieldStats>, HttpQueryError> {
    let planned = crate::querier::analytics::authorize_and_prepare_analytics_read(
        state,
        security,
        headers,
        TenantErrorSurface::QuerierRead,
        params.start,
        params.end,
        &params.query,
        VolumeRangeLimit::Check,
    )
    .await?;
    let read = planned.read(RangeEnd::Inclusive);

    let mut fields = BTreeMap::new();
    let mut scanned_lines = 0_usize;
    for block in &planned.plan.blocks {
        if scanned_lines >= params.line_limit {
            break;
        }
        let Some(rows) = read_planned_log_block(&planned.state, &block.key).await? else {
            continue;
        };
        for row in rows {
            if scanned_lines >= params.line_limit {
                break;
            }
            if !reads_planned_row(&read, &row)? {
                continue;
            }
            scanned_lines += 1;
            detect_entry_fields(&mut fields, &row.line, &row.structured_metadata);
        }
    }

    for record in plan_hot_tail_records(&planned.state, &planned.plan) {
        if scanned_lines >= params.line_limit {
            break;
        }
        if is_deleted_log_entry(
            read.delete_filters,
            &record.labels,
            &record.line,
            &record.structured_metadata,
            record.timestamp_ns,
        ) || !planned.plan.query.matches_with_fields(
            &record.labels,
            &record.line,
            &record.structured_metadata,
        ) {
            continue;
        }
        scanned_lines += 1;
        detect_entry_fields(&mut fields, &record.line, &record.structured_metadata);
    }

    Ok(fields)
}
