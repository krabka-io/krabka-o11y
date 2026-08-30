use super::*;

pub(crate) fn parse_leading_metric_vector_group_modifier(
    query: &str,
) -> Option<(Option<MetricVectorGroupModifier>, &str)> {
    for modifier in ["group_left", "group_right"] {
        let Some(rest) = query.strip_prefix(modifier) else {
            continue;
        };
        let rest = rest.trim_start();
        let (labels, rest) = if rest.starts_with('(') {
            parse_leading_label_list(rest)?
        } else {
            (Vec::new(), rest)
        };
        let group = match modifier {
            "group_left" => MetricVectorGroupModifier::Left(labels),
            "group_right" => MetricVectorGroupModifier::Right(labels),
            _ => unreachable!("modifier loop only produces known group modifiers"),
        };
        return Some((Some(group), rest));
    }

    Some((None, query))
}
