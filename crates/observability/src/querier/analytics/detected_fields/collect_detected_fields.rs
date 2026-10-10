use super::{
    AnalyticsQuery, BTreeMap, DetectedFieldStats, DetectedFieldsParams, HeaderMap, HttpQueryError,
    PlannedRead, QuerierState, RangeEnd, RequestSecurity, TenantErrorSurface, TimeRange,
    VolumeRangeLimit, active_log_delete_filters, authorized_tenant, detect_entry_fields,
    is_deleted_log_entry, plan_analytics_query, plan_hot_tail_records, read_planned_log_block,
    reads_planned_row,
};

pub(crate) async fn collect_detected_fields(
    state: &QuerierState,
    security: &RequestSecurity,
    headers: &HeaderMap,
    params: &DetectedFieldsParams,
) -> Result<BTreeMap<String, DetectedFieldStats>, HttpQueryError> {
    let tenant =
        authorized_tenant(state, security, headers, TenantErrorSurface::QuerierRead).await?;
    let time_range = TimeRange::new(params.start, params.end)?;
    let (state, plan, time_range) = plan_analytics_query(
        state,
        AnalyticsQuery {
            tenant: &tenant,
            time_range,
            query: &params.query,
            volume_range_limit: VolumeRangeLimit::Check,
        },
    )
    .await?;
    let tenant = tenant.as_str();
    let delete_filters = active_log_delete_filters(&state, tenant, time_range)?;
    let read = PlannedRead {
        state: &state,
        plan: &plan,
        delete_filters: &delete_filters,
        end: RangeEnd::Inclusive,
    };

    let mut fields = BTreeMap::new();
    let mut scanned_lines = 0_usize;
    for block in &plan.blocks {
        if scanned_lines >= params.line_limit {
            break;
        }
        let Some(rows) = read_planned_log_block(&state, &block.key).await? else {
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

    for record in plan_hot_tail_records(&state, &plan) {
        if scanned_lines >= params.line_limit {
            break;
        }
        if is_deleted_log_entry(
            &delete_filters,
            &record.labels,
            &record.line,
            &record.structured_metadata,
            record.timestamp_ns,
        ) || !plan.query.matches_with_fields(
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
