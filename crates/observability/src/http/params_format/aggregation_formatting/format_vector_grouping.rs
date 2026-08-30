use super::VectorGrouping;

pub(crate) fn format_vector_grouping(grouping: &VectorGrouping) -> String {
    match grouping {
        VectorGrouping::By(labels) => format!("by ({})", labels.join(",")),
        VectorGrouping::Without(labels) => format!("without ({})", labels.join(",")),
    }
}
