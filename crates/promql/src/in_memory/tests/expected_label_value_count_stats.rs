use super::*;

pub(crate) fn expected_label_value_count_stats() -> Vec<NamedTsdbStat> {
    vec![
        NamedTsdbStat {
            name: "__name__".to_string(),
            value: 2,
        },
        NamedTsdbStat {
            name: "job".to_string(),
            value: 2,
        },
    ]
}
