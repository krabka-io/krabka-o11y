use super::{
    DistributorError, Labels, Limits, is_loki_label_name, loki_push_label_parse_error,
    validate_loki_label_limits,
};

/// Checks a pushed stream's labels for `Loki`'s name syntax and then for the
/// tenant's three label caps.
///
/// Syntax first, as `Loki` does: a label name that is not a name at all is not
/// a limit failure, and its message names the offending character.
pub(crate) fn validate_loki_stream_labels(
    labels: &Labels,
    limits: &Limits,
) -> Result<(), DistributorError> {
    if let Some(name) = labels.keys().find(|name| !is_loki_label_name(name)) {
        return Err(DistributorError::InvalidPushLabelSyntax(
            loki_push_label_parse_error(labels, name),
        ));
    }
    validate_loki_label_limits(labels, limits)
}
