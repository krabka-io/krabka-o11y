use super::*;

pub(crate) fn merge_ingest_limits(
    base: &crate::ingest::TenantLimits,
    overrides: &Limits,
) -> crate::ingest::TenantLimits {
    crate::ingest::TenantLimits {
        max_label_name: positive_or(overrides.max_label_name, base.max_label_name),
        max_label_names_per_series: usize::try_from(overrides.max_label_names_per_series)
            .ok()
            .filter(|limit| *limit > 0)
            .unwrap_or(base.max_label_names_per_series),
        max_label_value: positive_or(overrides.max_label_value, base.max_label_value),
        session_id_buckets: if overrides.max_session_id_cardinality > 0 {
            overrides.max_session_id_cardinality
        } else {
            base.session_id_buckets
        },
    }
}
