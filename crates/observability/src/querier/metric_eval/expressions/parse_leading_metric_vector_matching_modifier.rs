use super::{
    MetricVectorMatching, parse_leading_label_list, parse_leading_metric_vector_group_modifier,
};

pub(crate) struct ParsedMetricVectorMatchingModifier<'a> {
    pub(crate) matching: Option<MetricVectorMatching>,
    pub(crate) remaining: &'a str,
}

pub(crate) fn parse_leading_metric_vector_matching_modifier(
    query: &str,
    allow_group_modifier: bool,
) -> Option<ParsedMetricVectorMatchingModifier<'_>> {
    let query = query.trim_start();
    for modifier in ["on", "ignoring"] {
        let Some(rest) = query.strip_prefix(modifier) else {
            continue;
        };
        let parsed_labels = parse_leading_label_list(rest.trim_start())?;
        let parsed_group =
            parse_leading_metric_vector_group_modifier(parsed_labels.remaining.trim_start())?;
        let labels = parsed_labels.labels;
        let rest = parsed_group.remaining;
        let group = parsed_group.group;
        if group.is_some() && !allow_group_modifier {
            return None;
        }
        let matching = match modifier {
            "on" => MetricVectorMatching::On { labels, group },
            "ignoring" => MetricVectorMatching::Ignoring { labels, group },
            _ => unreachable!("modifier loop only produces known modifiers"),
        };
        return Some(ParsedMetricVectorMatchingModifier {
            matching: Some(matching),
            remaining: rest,
        });
    }

    Some(ParsedMetricVectorMatchingModifier {
        matching: None,
        remaining: query,
    })
}
