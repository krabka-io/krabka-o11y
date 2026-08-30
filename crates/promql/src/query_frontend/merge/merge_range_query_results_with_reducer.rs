use super::{
    BTreeMap, PromqlError, QueryResult, QueryShardReducer, RangeSeries, SeriesFingerprint,
    label_sort_key, reduce_duplicate_step_samples,
};

pub(crate) fn merge_range_query_results_with_reducer(
    results: Vec<QueryResult>,
    reducer: QueryShardReducer,
) -> Result<QueryResult, PromqlError> {
    let mut by_fp = BTreeMap::<SeriesFingerprint, RangeSeries>::new();

    for result in results {
        let QueryResult::RangeMatrix(series) = result else {
            return Err(PromqlError::Plan(
                "query-frontend range merge requires range matrix subquery results".into(),
            ));
        };
        for mut series in series {
            by_fp
                .entry(series.labels.fingerprint())
                .and_modify(|existing| existing.samples.append(&mut series.samples))
                .or_insert(series);
        }
    }

    let mut series = by_fp.into_values().collect::<Vec<_>>();
    series.sort_by_key(|series| label_sort_key(&series.labels));
    for series in &mut series {
        series.samples.sort_by_key(|(ts_ms, _)| *ts_ms);
        reduce_duplicate_step_samples(&mut series.samples, reducer)?;
    }
    Ok(QueryResult::RangeMatrix(series))
}
