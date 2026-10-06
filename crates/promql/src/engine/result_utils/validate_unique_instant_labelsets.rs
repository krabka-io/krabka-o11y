use super::{BTreeSet, PromqlError, QueryResult, Result, labels_key};
use crate::PromqlLabels as Labels;

pub(crate) fn validate_unique_instant_labelsets(result: &QueryResult) -> Result<()> {
    match result {
        QueryResult::InstantVector(samples) => {
            validate_labels(samples.iter().map(|sample| &sample.labels))
        }
        QueryResult::RangeMatrix(series) => {
            validate_labels(series.iter().map(|series| &series.labels))
        }
        QueryResult::Scalar { .. } | QueryResult::Str { .. } => Ok(()),
    }
}

fn validate_labels<'a>(labels: impl Iterator<Item = &'a Labels>) -> Result<()> {
    let mut seen = BTreeSet::new();
    for labels in labels {
        let key = labels_key(labels);
        if !seen.insert(key.clone()) {
            return Err(PromqlError::Exec(format!(
                "vector cannot contain metrics with the same labelset: {key}"
            )));
        }
    }
    Ok(())
}
