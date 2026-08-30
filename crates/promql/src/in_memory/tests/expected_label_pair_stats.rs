use super::*;

pub(crate) fn expected_label_pair_stats() -> Vec<NamedTsdbStat> {
    vec![
        NamedTsdbStat {
            name: "__name__=up".to_string(),
            value: 2,
        },
        NamedTsdbStat {
            name: "job=api".to_string(),
            value: 2,
        },
        NamedTsdbStat {
            name: "__name__=latency_seconds".to_string(),
            value: 1,
        },
        NamedTsdbStat {
            name: "job=worker".to_string(),
            value: 1,
        },
    ]
}
