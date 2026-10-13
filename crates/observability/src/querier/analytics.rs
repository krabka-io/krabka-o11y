pub(crate) mod detected_fields;
pub(crate) mod index_patterns;

use detected_fields::{
    PlannedAnalyticsQuery, PlannedRead, RangeEnd, VolumeRangeLimit, plan_analytics_query,
};

use crate::{
    ActiveLogDeleteFilter, HeaderMap, HttpQueryError, QuerierState, RequestSecurity,
    TenantErrorSurface, TenantId, TimeRange, active_log_delete_filters, authorized_tenant,
};

pub(crate) async fn authorize_and_plan_analytics_query(
    state: &QuerierState,
    security: &RequestSecurity,
    headers: &HeaderMap,
    error_surface: TenantErrorSurface,
    start: i64,
    end: i64,
    query: &str,
    volume_range_limit: VolumeRangeLimit,
) -> Result<(TenantId, PlannedAnalyticsQuery), HttpQueryError> {
    let tenant = authorized_tenant(state, security, headers, error_surface).await?;
    let time_range = TimeRange::new(start, end)?;
    let planned = plan_analytics_query(
        state,
        detected_fields::AnalyticsQuery {
            tenant: &tenant,
            time_range,
            query,
            volume_range_limit,
        },
    )
    .await?;
    Ok((tenant, planned))
}

pub(crate) struct PlannedAnalyticsRead {
    pub(crate) state: QuerierState,
    pub(crate) plan: crate::StreamPlan,
    delete_filters: Vec<ActiveLogDeleteFilter>,
}

impl PlannedAnalyticsRead {
    pub(crate) fn read(&self, end: RangeEnd) -> PlannedRead<'_> {
        PlannedRead {
            state: &self.state,
            plan: &self.plan,
            delete_filters: &self.delete_filters,
            end,
        }
    }
}

pub(crate) async fn authorize_and_prepare_analytics_read(
    state: &QuerierState,
    security: &RequestSecurity,
    headers: &HeaderMap,
    error_surface: TenantErrorSurface,
    start: i64,
    end: i64,
    query: &str,
    volume_range_limit: VolumeRangeLimit,
) -> Result<PlannedAnalyticsRead, HttpQueryError> {
    let (tenant, planned) = authorize_and_plan_analytics_query(
        state,
        security,
        headers,
        error_surface,
        start,
        end,
        query,
        volume_range_limit,
    )
    .await?;
    let time_range = planned.time_range;
    let delete_filters = active_log_delete_filters(&planned.state, tenant.as_str(), time_range)?;
    Ok(PlannedAnalyticsRead {
        state: planned.state,
        plan: planned.plan,
        delete_filters,
    })
}
