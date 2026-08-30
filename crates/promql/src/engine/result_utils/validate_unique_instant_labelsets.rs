use super::{BTreeSet, PromqlError, QueryResult, Result, labels_key};

pub(crate) fn validate_unique_instant_labelsets(result: &QueryResult) -> Result<()> {
    let QueryResult::InstantVector(samples) = result else {
        return Ok(());
    };
    let mut seen = BTreeSet::new();
    for sample in samples {
        let key = labels_key(&sample.labels);
        if !seen.insert(key.clone()) {
            return Err(PromqlError::Exec(format!(
                "vector cannot contain metrics with the same labelset: {key}"
            )));
        }
    }
    Ok(())
}
