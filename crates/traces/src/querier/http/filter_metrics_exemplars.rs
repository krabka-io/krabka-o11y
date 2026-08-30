use super::*;

pub(crate) fn filter_metrics_exemplars(
    mut resp: TraceMetricsResponse,
    selection: ExemplarSelection,
) -> TraceMetricsResponse {
    match selection {
        ExemplarSelection::All => {}
        ExemplarSelection::Limit(max) => {
            for series in &mut resp.series {
                series.exemplars.truncate(max);
            }
        }
        ExemplarSelection::None => {
            for series in &mut resp.series {
                series.exemplars.clear();
            }
        }
    }
    resp
}
