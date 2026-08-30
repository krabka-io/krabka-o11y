use super::{QueryResult, Result, BTreeSet, labels_key, PromqlError};

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
