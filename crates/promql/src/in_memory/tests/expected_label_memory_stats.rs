use super::*;

pub(crate) fn expected_label_memory_stats() -> Vec<NamedTsdbStat> {
    vec![
        NamedTsdbStat {
            name: "__name__".to_string(),
            value: 43,
        },
        NamedTsdbStat {
            name: "job".to_string(),
            value: 21,
        },
    ]
}
