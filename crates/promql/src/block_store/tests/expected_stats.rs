use super::*;

pub(crate) fn expected_stats(pairs: &[(&str, usize)]) -> Vec<NamedTsdbStat> {
    pairs
        .iter()
        .map(|(name, value)| NamedTsdbStat {
            name: (*name).to_string(),
            value: *value,
        })
        .collect()
}
