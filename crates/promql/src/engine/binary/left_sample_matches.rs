use std::collections::BTreeMap;

use super::{BinModifier, InstantSample, binary_match_key};

pub(super) struct LeftSampleMatch<'a> {
    pub(super) left: InstantSample,
    pub(super) key: String,
    pub(super) right: Option<&'a InstantSample>,
}

pub(super) fn left_sample_matches<'a>(
    left: Vec<InstantSample>,
    right_by_key: &'a BTreeMap<String, InstantSample>,
    modifier: Option<&'a BinModifier>,
) -> impl Iterator<Item = LeftSampleMatch<'a>> + 'a {
    left.into_iter().map(move |left| {
        let key = binary_match_key(&left.labels, modifier);
        let right = right_by_key.get(&key);
        LeftSampleMatch { left, key, right }
    })
}
