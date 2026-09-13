use super::{MetricStore, PrometheusApiState, RulerStateWalRecord};

/// Applies a restored ruler group or alert record to the API state.
pub fn apply_ruler_state_record<S: MetricStore>(
    state: &PrometheusApiState<S>,
    record: RulerStateWalRecord,
) {
    match record {
        RulerStateWalRecord::Group(record) => state.apply_ruler_group_state(record),
        RulerStateWalRecord::Alert(record) => state.apply_ruler_alert_state(record),
    }
}
