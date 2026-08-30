use super::*;

pub(crate) fn parse_loki_proto_labels(labels: &str) -> Result<Labels, DistributorError> {
    let labels = labels.trim();
    if labels.is_empty() || labels == "{}" {
        return Ok(Labels::new());
    }

    let query = parse_query(labels).map_err(|_| {
        loki_proto_label_parse_error(labels).map_or(
            DistributorError::InvalidPushLabels,
            DistributorError::InvalidPushLabelSyntax,
        )
    })?;
    if !query.pipeline.is_empty() {
        return Err(DistributorError::InvalidPushLabels);
    }

    let mut labels = Labels::new();
    let mut rendered_labels = Vec::new();
    for matcher in query.matchers {
        if matcher.op != MatchOp::Equal {
            return Err(DistributorError::InvalidPushLabels);
        }
        rendered_labels.push(format!(
            "{}={}",
            matcher.name,
            quote_logql_string(&matcher.value)
        ));
        if labels.contains_key(&matcher.name) {
            let mut discovered_labels = labels.clone();
            discover_service_name_label(&mut discovered_labels);
            if !rendered_labels
                .iter()
                .any(|label| label.starts_with("service_name="))
                && let Some(service_name) = discovered_labels.get("service_name")
            {
                rendered_labels.push(format!("service_name={}", quote_logql_string(service_name)));
            }
            return Err(DistributorError::InvalidPushLabelSyntax(format!(
                "stream '{{{}}}' has duplicate label name: '{}'\n",
                rendered_labels.join(", "),
                matcher.name
            )));
        }
        labels.insert(matcher.name, matcher.value);
    }

    Ok(labels)
}
