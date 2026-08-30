use super::*;

pub(crate) fn split_query_param_pairs<'a>(raw_query: &'a str, known_keys: &[&str]) -> Vec<&'a str> {
    let mut pairs = Vec::new();
    let mut pair_start = 0;
    for (index, byte) in raw_query.bytes().enumerate() {
        if byte == b'&'
            && known_keys.iter().any(|key| {
                raw_query[index + 1..]
                    .strip_prefix(key)
                    .is_some_and(|rest| rest.starts_with('='))
            })
        {
            if pair_start != index {
                pairs.push(&raw_query[pair_start..index]);
            }
            pair_start = index + 1;
        }
    }
    if pair_start < raw_query.len() {
        pairs.push(&raw_query[pair_start..]);
    }
    pairs
}
