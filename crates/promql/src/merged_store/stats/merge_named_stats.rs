use super::{BTreeMap, NamedTsdbStat};
use crate::series_stats::named_stats;

pub(crate) fn merge_named_stats(
    left: Vec<NamedTsdbStat>,
    right: Vec<NamedTsdbStat>,
) -> Vec<NamedTsdbStat> {
    let mut values = BTreeMap::<String, usize>::new();
    for stat in left.into_iter().chain(right) {
        *values.entry(stat.name).or_default() += stat.value;
    }
    named_stats(values)
}
