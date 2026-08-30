use super::{
    DistributorError, HeaderMap, ProtoExportLogsServiceRequest, Time, WalLogRecord,
    normalize_otlp_proto_logs_for_tenant, tenant,
};

pub(crate) fn normalize_otlp_proto_logs(
    headers: &HeaderMap,
    payload: ProtoExportLogsServiceRequest,
    reject_old_samples_max_age: Option<Time>,
    creation_grace_period: Option<Time>,
) -> Result<Vec<WalLogRecord>, DistributorError> {
    let tenant = tenant(headers)?;
    normalize_otlp_proto_logs_for_tenant(
        tenant,
        payload,
        reject_old_samples_max_age,
        creation_grace_period,
    )
}
