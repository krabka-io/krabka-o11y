use super::{fmt, MetricVectorMatching, MetricVectorGroupModifier, format_labels};

pub(crate) fn format_matching(
    f: &mut fmt::Formatter<'_>,
    matching: Option<&MetricVectorMatching>,
) -> fmt::Result {
    let Some(m) = matching else { return Ok(()) };
    let (labels, group, name) = match m {
        MetricVectorMatching::On { labels, group } => (labels, group, "on"),
        MetricVectorMatching::Ignoring { labels, group } => (labels, group, "ignoring"),
    };
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
