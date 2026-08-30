use super::*;

pub(crate) fn parse_leading_metric_vector_matching_modifier(
    query: &str,
    allow_group_modifier: bool,
) -> Option<(Option<MetricVectorMatching>, &str)> {
    let query = query.trim_start();
    for modifier in ["on", "ignoring"] {
        let Some(rest) = query.strip_prefix(modifier) else {
            continue;
        };
        let (labels, rest) = parse_leading_label_list(rest.trim_start())?;
        let (group, rest) = parse_leading_metric_vector_group_modifier(rest.trim_start())?;
        if group.is_some() && !allow_group_modifier {
            return None;
        }
        let matching = match modifier {
            "on" => MetricVectorMatching::On { labels, group },
            "ignoring" => MetricVectorMatching::Ignoring { labels, group },
            _ => unreachable!("modifier loop only produces known modifiers"),
        };
        return Some((Some(matching), rest));
    }

    Some((None, query))
}
