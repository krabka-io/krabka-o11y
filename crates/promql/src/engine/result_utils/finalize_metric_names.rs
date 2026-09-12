use super::QueryResult;

pub(crate) fn finalize_metric_names(result: &mut QueryResult) {
    let QueryResult::InstantVector(samples) = result else {
        return;
    };
    for sample in samples {
        if !sample.drop_name {
            continue;
        }
        sample.labels = sample
            .labels
            .iter()
            .filter(|(name, _)| name.as_str() != "__name__")
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect();
        sample.drop_name = false;
    }
}
