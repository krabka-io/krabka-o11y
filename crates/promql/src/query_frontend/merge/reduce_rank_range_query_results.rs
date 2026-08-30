use super::{BTreeMap, BTreeSet, LabelModifier, PromqlError, QueryResult, RankCandidate, RankReduction, SampleValue, SeriesFingerprint, aggregate_labels, compare_rank_candidates, label_sort_key};

pub(crate) fn reduce_rank_range_query_results(
    result: QueryResult,
    k: usize,
    kind: RankReduction,
    modifier: Option<&LabelModifier>,
) -> Result<QueryResult, PromqlError> {
    let QueryResult::RangeMatrix(mut series) = result else {
        return Err(PromqlError::Plan(
            "rank query-frontend merge requires range matrix results".into(),
        ));
    };
    if k == 0 {
        return Ok(QueryResult::RangeMatrix(Vec::new()));
    }

    let mut keep = BTreeSet::<(SeriesFingerprint, usize)>::new();
    let mut candidates_by_step_and_group = BTreeMap::<(i64, String), Vec<RankCandidate>>::new();
    for (series_index, series) in series.iter().enumerate() {
        let group = label_sort_key(&aggregate_labels(&series.labels, modifier));
        let labels_key = label_sort_key(&series.labels);
        let fingerprint = series.labels.fingerprint();
        for (sample_index, (ts_ms, value)) in series.samples.iter().enumerate() {
            if let SampleValue::Float(value) = value {
                candidates_by_step_and_group
                    .entry((*ts_ms, group.clone()))
                    .or_default()
                    .push(RankCandidate {
                        fingerprint,
                        labels_key: labels_key.clone(),
                        sample_index,
                        series_index,
                        value: *value,
                    });
            }
        }
    }

    for mut candidates in candidates_by_step_and_group.into_values() {
        candidates.sort_by(|left, right| compare_rank_candidates(kind, left, right));
        candidates.truncate(k.min(candidates.len()));
        keep.extend(
            candidates
                .into_iter()
                .map(|candidate| (candidate.fingerprint, candidate.sample_index)),
        );
    }

    for series in &mut series {
        let fingerprint = series.labels.fingerprint();
        let mut sample_index = 0_usize;
        series.samples.retain(|_| {
            let keep_sample = keep.contains(&(fingerprint, sample_index));
            sample_index += 1;
            keep_sample
        });
    }
    series.retain(|series| !series.samples.is_empty());
    series.sort_by_key(|series| label_sort_key(&series.labels));
    Ok(QueryResult::RangeMatrix(series))
}
