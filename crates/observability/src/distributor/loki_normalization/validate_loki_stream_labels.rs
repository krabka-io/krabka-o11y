use super::*;

pub(crate) fn validate_loki_stream_labels(labels: &Labels) -> Result<(), DistributorError> {
    if let Some(name) = labels.keys().find(|name| !is_loki_label_name(name)) {
        return Err(DistributorError::InvalidPushLabelSyntax(
            loki_push_label_parse_error(labels, name),
        ));
    }
    Ok(())
}
