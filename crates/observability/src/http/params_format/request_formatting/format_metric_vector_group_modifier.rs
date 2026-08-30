use super::{MetricVectorGroupModifier, format_metric_vector_group_modifier_text};

pub(crate) fn format_metric_vector_group_modifier(group: &MetricVectorGroupModifier) -> String {
    match group {
        MetricVectorGroupModifier::Left(labels) => {
            format_metric_vector_group_modifier_text("group_left", labels)
        }
        MetricVectorGroupModifier::Right(labels) => {
            format_metric_vector_group_modifier_text("group_right", labels)
        }
    }
}
