use super::*;

pub(crate) fn expected_label_value_cardinality() -> Vec<LabelValueCardinality> {
    vec![
        LabelValueCardinality {
            label_name: "__name__".to_string(),
            label_value: "up".to_string(),
            series_count: 2,
        },
        LabelValueCardinality {
            label_name: "job".to_string(),
            label_value: "api".to_string(),
            series_count: 2,
        },
        LabelValueCardinality {
            label_name: "__name__".to_string(),
            label_value: "latency_seconds".to_string(),
            series_count: 1,
        },
        LabelValueCardinality {
            label_name: "job".to_string(),
            label_value: "worker".to_string(),
            series_count: 1,
        },
    ]
}
