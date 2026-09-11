use super::{
    DistributorError, Limits, ProtoExportLogsServiceRequest, TenantId, WalLogRecord,
    normalize_otlp_proto_logs_for_tenant,
};

pub(crate) fn normalize_otlp_proto_logs(
    tenant: &TenantId,
    payload: ProtoExportLogsServiceRequest,
    limits: &Limits,
) -> Result<Vec<WalLogRecord>, DistributorError> {
    normalize_otlp_proto_logs_for_tenant(tenant, payload, limits)
}
