use std::collections::BTreeMap;

pub(crate) struct ShardStringDictionary {
    pub(crate) strings: Vec<String>,
    pub(crate) ids: BTreeMap<String, usize>,
}

pub(crate) fn assign_sorted_string_ids(entries: BTreeMap<String, usize>) -> ShardStringDictionary {
    let strings = entries.into_keys().collect::<Vec<_>>();
    let ids = strings
        .iter()
        .enumerate()
        .map(|(id, text)| (text.clone(), id))
        .collect();
    ShardStringDictionary { strings, ids }
}
