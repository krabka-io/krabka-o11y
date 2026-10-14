use super::{MetricVectorGroupModifier, MetricVectorMatching, fmt, format_labels};

pub(crate) fn format_matching(
    f: &mut fmt::Formatter<'_>,
    matching: Option<&MetricVectorMatching>,
) -> fmt::Result {
    let Some(m) = matching else { return Ok(()) };
    match m {
        MetricVectorMatching::On { labels, group } => {
            format_matching_parts(f, labels, group.as_ref(), "on")
        }
        MetricVectorMatching::Ignoring { labels, group } => {
            format_matching_parts(f, labels, group.as_ref(), "ignoring")
        }
    }
}

fn format_matching_parts(
    f: &mut fmt::Formatter<'_>,
    labels: &[String],
    group: Option<&MetricVectorGroupModifier>,
    name: &str,
) -> fmt::Result {
    write!(f, " {name}({})", labels.join(", "))?;
    if let Some(group) = group {
        match group {
            MetricVectorGroupModifier::Left(labels) => {
                write!(f, " group_left{}", format_labels(labels))?;
            }
            MetricVectorGroupModifier::Right(labels) => {
                write!(f, " group_right{}", format_labels(labels))?;
            }
        }
    }
    Ok(())
}
