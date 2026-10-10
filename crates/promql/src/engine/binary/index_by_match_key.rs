use super::{BTreeMap, BinModifier, InstantSample, PromqlError, Result, binary_match_key};

/// Indexes `samples` by their vector-matching key, and rejects a key that two
/// samples share with the message `duplicate_message` builds from that key.
pub(crate) fn index_by_match_key(
    samples: Vec<InstantSample>,
    modifier: Option<&BinModifier>,
    duplicate_message: impl Fn(&str) -> String,
) -> Result<BTreeMap<String, InstantSample>> {
    let mut by_key: BTreeMap<String, InstantSample> = BTreeMap::new();
    for sample in samples {
        let key = binary_match_key(&sample.labels, modifier);
        if by_key.insert(key.clone(), sample).is_some() {
            return Err(PromqlError::Exec(duplicate_message(&key)));
        }
    }
    Ok(by_key)
}
