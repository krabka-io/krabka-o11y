use super::*;

pub(crate) fn expected_label_name_cardinality() -> Vec<LabelNameCardinality> {
    vec![
        LabelNameCardinality {
            name: "__name__".to_string(),
            series_count: 3,
        },
        LabelNameCardinality {
            name: "job".to_string(),
            series_count: 3,
        },
    ]
}
