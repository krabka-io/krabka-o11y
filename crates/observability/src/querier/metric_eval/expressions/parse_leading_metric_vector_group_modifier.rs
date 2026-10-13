use super::{MetricVectorGroupModifier, parse_leading_label_list};

pub(crate) struct ParsedMetricVectorGroupModifier<'a> {
    pub(crate) group: Option<MetricVectorGroupModifier>,
    pub(crate) remaining: &'a str,
}

pub(crate) fn parse_leading_metric_vector_group_modifier(
    query: &str,
) -> Option<ParsedMetricVectorGroupModifier<'_>> {
    for modifier in ["group_left", "group_right"] {
        let Some(rest) = query.strip_prefix(modifier) else {
            continue;
        };
        let rest = rest.trim_start();
        let labels;
        let remaining;
        if rest.starts_with('(') {
            let parsed = parse_leading_label_list(rest)?;
            labels = parsed.labels;
            remaining = parsed.remaining;
        } else {
            labels = Vec::new();
            remaining = rest;
        }
        let group = match modifier {
            "group_left" => MetricVectorGroupModifier::Left(labels),
            "group_right" => MetricVectorGroupModifier::Right(labels),
            _ => unreachable!("modifier loop only produces known group modifiers"),
        };
        return Some(ParsedMetricVectorGroupModifier {
            group: Some(group),
            remaining,
        });
    }

    Some(ParsedMetricVectorGroupModifier {
        group: None,
        remaining: query,
    })
}
