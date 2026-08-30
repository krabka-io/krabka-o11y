use super::{QueryResult, MomentReduction, PromqlError, float_samples_by_fingerprint, SampleValue, RangeSeries};

pub(crate) fn reduce_moment_range_query_results(
    sums: QueryResult,
    counts: QueryResult,
    sum_squares: QueryResult,
    kind: MomentReduction,
) -> Result<QueryResult, PromqlError> {
    let QueryResult::RangeMatrix(sum_series) = sums else {
        return Err(PromqlError::Plan(
            "moment query-frontend sum merge requires range matrix results".into(),
        ));
    };
    let QueryResult::RangeMatrix(count_series) = counts else {
        return Err(PromqlError::Plan(
            "moment query-frontend count merge requires range matrix results".into(),
        ));
    };
    let QueryResult::RangeMatrix(sum_squares_series) = sum_squares else {
        return Err(PromqlError::Plan(
            "moment query-frontend sum-squares merge requires range matrix results".into(),
        ));
    };
    let counts_by_fp = float_samples_by_fingerprint(count_series);
    let sum_squares_by_fp = float_samples_by_fingerprint(sum_squares_series);
    let mut out_series = Vec::new();

    for series in sum_series {
        let fingerprint = series.labels.fingerprint();
        let Some(counts_by_ts) = counts_by_fp.get(&fingerprint) else {
            continue;
        };
        let Some(sum_squares_by_ts) = sum_squares_by_fp.get(&fingerprint) else {
            continue;
        };
        let samples = series
            .samples
            .into_iter()
            .filter_map(|(ts_ms, value)| {
                let SampleValue::Float(sum) = value else {
                    return None;
                };
                let count = *counts_by_ts.get(&ts_ms)?;
                let sum_squares = *sum_squares_by_ts.get(&ts_ms)?;
                if count == 0.0 {
                    return None;
                }
                let mean = sum / count;
                let variance = ((sum_squares / count) - (mean * mean)).max(0.0);
                let value = match kind {
                    MomentReduction::Stddev => variance.sqrt(),
                    MomentReduction::Stdvar => variance,
                };
                Some((ts_ms, SampleValue::Float(value)))
            })
            .collect::<Vec<_>>();
        if !samples.is_empty() {
            out_series.push(RangeSeries {
                labels: series.labels,
                samples,
            });
        }
    }

    Ok(QueryResult::RangeMatrix(out_series))
}
