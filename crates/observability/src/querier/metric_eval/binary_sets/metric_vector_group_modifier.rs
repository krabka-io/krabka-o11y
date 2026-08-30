use super::*;

pub(crate) fn metric_vector_group_modifier(
    matching: Option<&MetricVectorMatching>,
) -> Option<&MetricVectorGroupModifier> {
    match matching {
        Some(
            MetricVectorMatching::On { group, .. } | MetricVectorMatching::Ignoring { group, .. },
        ) => group.as_ref(),
        None => None,
    }
}
