use super::*;

pub(crate) fn expected_metric_name_stats() -> Vec<NamedTsdbStat> {
    vec![
        NamedTsdbStat {
            name: "up".to_string(),
            value: 2,
        },
        NamedTsdbStat {
            name: "latency_seconds".to_string(),
            value: 1,
        },
    ]
}
