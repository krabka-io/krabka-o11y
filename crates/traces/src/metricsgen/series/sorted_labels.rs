use super::*;

/// Sort labels into a deterministic order for the encoder and the tests.
#[must_use]
pub fn sorted_labels(mut pairs: Vec<(String, String)>) -> Vec<(String, String)> {
    pairs.sort();
    pairs
}
