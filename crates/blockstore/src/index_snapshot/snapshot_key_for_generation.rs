use super::index_snapshot_prefix_for_key;

/// Object key for one generation of an index snapshot.
///
/// The generation is a plain counter, and the next writer derives its key from
/// the newest generation it can see, so two writers that start from the same
/// state mint the *same* key and a conditional create lets only one of them
/// through. Zero-padding to 20 digits, `u64`'s widest decimal form, keeps the
/// lexicographic order of the keys equal to the numeric order of the
/// generations, so listing the prefix and taking the last entry still finds
/// the newest snapshot.
pub(crate) fn snapshot_key_for_generation(key: &str, generation: u64) -> String {
    format!(
        "{}/{generation:020}.json",
        index_snapshot_prefix_for_key(key)
    )
}
