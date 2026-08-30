use super::{BTreeMap, NamedTsdbStat};

pub(crate) fn merge_named_stats(
    left: Vec<NamedTsdbStat>,
    right: Vec<NamedTsdbStat>,
) -> Vec<NamedTsdbStat> {
    let mut values = BTreeMap::<String, usize>::new();
    for stat in left.into_iter().chain(right) {
        *values.entry(stat.name).or_default() += stat.value;
    }
    let mut out = values
        .into_iter()
        .map(|(name, value)| NamedTsdbStat { name, value })
        .collect::<Vec<_>>();
    out.sort_by(|left, right| {
        right
            .value
            .cmp(&left.value)
            .then_with(|| left.name.cmp(&right.name))
    });
    out
}
