use std::hash::Hash;

use dashmap::DashMap;

pub(crate) fn evict_until_within_cap<K, V>(
    entries: &DashMap<K, V>,
    max_entries: usize,
    last_touch: impl Fn(&V) -> u64,
) where
    K: Clone + Eq + Hash,
{
    while entries.len() > max_entries {
        let oldest = entries
            .iter()
            .min_by_key(|entry| last_touch(entry.value()))
            .map(|entry| entry.key().clone());
        match oldest {
            Some(key) => {
                entries.remove(&key);
            }
            None => break,
        }
    }
}
