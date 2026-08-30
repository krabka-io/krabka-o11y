use super::*;

#[must_use]
pub fn stream_plan_scan_sql(plan: &StreamPlan) -> String {
    stream_plan_scan_sql_for_time_range(plan, plan.time_range)
}
