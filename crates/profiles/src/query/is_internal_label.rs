use super::*;

/// Labels that Krabka stores internally for span and exemplar lookups.
///
/// Pyroscope does not expose these labels through the label-enumeration APIs
/// `LabelNames` and `LabelValues`, or through series counting. Krabka attaches
/// `__profile_id__` to each profile, so a report of it would leak per-profile
/// cardinality that real Pyroscope never reports.
pub(crate) fn is_internal_label(name: &str) -> bool {
    name == PROFILE_ID_LABEL
}
