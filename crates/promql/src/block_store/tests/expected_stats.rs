use super::*;

pub(crate) struct ExpectedStat {
    pub(crate) name: &'static str,
    pub(crate) value: usize,
}

pub(crate) fn expected_stats(stats: &[ExpectedStat]) -> Vec<NamedTsdbStat> {
    stats
        .iter()
        .map(|stat| NamedTsdbStat {
            name: stat.name.to_string(),
            value: stat.value,
        })
        .collect()
}
