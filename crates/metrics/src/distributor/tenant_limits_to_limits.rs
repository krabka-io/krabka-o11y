use super::*;

pub(crate) fn tenant_limits_to_limits(limits: &TenantLimits) -> Limits {
    Limits {
        ingestion_rate: limits.ingestion_rate,
        ingestion_burst_size: u64::try_from(limits.ingestion_burst_size).unwrap_or(u64::MAX),
        max_label_name_length: limits.max_label_name_len,
        max_label_value_length: limits.max_label_value_len,
        out_of_order_time_window: limits.out_of_order_time_window,
        ..Limits::default()
    }
}
