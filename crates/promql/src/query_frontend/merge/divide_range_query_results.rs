use super::{
    BTreeMap, PromqlError, QueryResult, RangeSeries, SampleValue, scaled_native_histogram,
};

pub(crate) fn divide_range_query_results(
    sums: QueryResult,
    counts: QueryResult,
) -> Result<QueryResult, PromqlError> {
    let QueryResult::RangeMatrix(sum_series) = sums else {
        return Err(PromqlError::Plan(
            "avg query-frontend sum merge requires range matrix results".into(),
        ));
    };
    let QueryResult::RangeMatrix(count_series) = counts else {
        return Err(PromqlError::Plan(
            "avg query-frontend count merge requires range matrix results".into(),
        ));
    };
    let counts_by_fp = count_series
        .into_iter()
        .map(|series| (series.labels.fingerprint(), series))
        .collect::<BTreeMap<_, _>>();
    let mut avg_series = Vec::new();

    for series in sum_series {
        let Some(count_series) = counts_by_fp.get(&series.labels.fingerprint()) else {
            continue;
        };
        let counts_by_ts = count_series
            .samples
            .iter()
            .filter_map(|(ts_ms, value)| match value {
                SampleValue::Float(value) => Some((*ts_ms, *value)),
                SampleValue::Histogram(_) => None,
            })
            .collect::<BTreeMap<_, _>>();
        let samples = series
            .samples
            .into_iter()
            .filter_map(|(ts_ms, value)| {
                let count = *counts_by_ts.get(&ts_ms)?;
                if count == 0.0 {
                    return None;
                }
                Some((
                    ts_ms,
                    match value {
                        SampleValue::Float(value) => SampleValue::Float(value / count),
                        SampleValue::Histogram(histogram) => {
                            SampleValue::Histogram(scaled_native_histogram(&histogram, 1.0 / count))
                        }
                    },
                ))
            })
            .collect::<Vec<_>>();
        if !samples.is_empty() {
            avg_series.push(RangeSeries {
                labels: series.labels,
                samples,
            });
        }
    }

    Ok(QueryResult::RangeMatrix(avg_series))
}
