use super::{
    BTreeMap, PromqlError, QueryResult, RangeSeries, RangeSeriesSample, SampleValue,
    scaled_native_histogram,
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
    let mut counts_by_fp = BTreeMap::new();
    for series in count_series {
        counts_by_fp.insert(series.labels.fingerprint(), series);
    }
    let mut avg_series = Vec::new();

    for series in sum_series {
        let Some(count_series) = counts_by_fp.get(&series.labels.fingerprint()) else {
            continue;
        };
        let mut counts_by_ts = BTreeMap::new();
        for sample in &count_series.samples {
            if let SampleValue::Float(value) = &sample.value {
                counts_by_ts.insert(sample.timestamp_ms, *value);
            }
        }
        let samples = series
            .samples
            .into_iter()
            .filter_map(|sample| {
                let ts_ms = sample.timestamp_ms;
                let value = sample.value;
                let count = *counts_by_ts.get(&ts_ms)?;
                if count == 0.0 {
                    return None;
                }
                Some(RangeSeriesSample::new(
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
                drop_name: false,
                start_timestamps_ms: std::collections::BTreeMap::new(),
                labels: series.labels,
                samples,
            });
        }
    }

    Ok(QueryResult::RangeMatrix(avg_series))
}
