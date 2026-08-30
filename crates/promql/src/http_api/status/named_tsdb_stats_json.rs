use super::*;

pub(crate) fn named_tsdb_stats_json(mut stats: Vec<NamedTsdbStat>, limit: Option<usize>) -> Vec<Value> {
    apply_limit(&mut stats, limit);
    stats
        .into_iter()
        .map(|stat| {
            json!({
                "name": stat.name,
                "value": stat.value,
            })
        })
        .collect()
}
