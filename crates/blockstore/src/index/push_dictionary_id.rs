use super::{BTreeMap, push_len};

/// Appends the id `dictionary` gives `text`.
///
/// A shard encoder puts every string it writes by id into its dictionary
/// first, so a miss is a bug in the encoder rather than in the input.
pub(crate) fn push_dictionary_id(
    out: &mut Vec<u8>,
    dictionary: &BTreeMap<String, usize>,
    text: &str,
) {
    let id = dictionary
        .get(text)
        .copied()
        .expect("every dictionary string was put in the dictionary");
    push_len(out, id);
}
