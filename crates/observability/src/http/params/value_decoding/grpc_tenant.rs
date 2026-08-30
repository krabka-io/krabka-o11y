pub(crate) fn grpc_tenant(metadata: &tonic::metadata::MetadataMap) -> Result<&str, tonic::Status> {
    metadata
        .get("x-scope-orgid")
        .ok_or_else(|| tonic::Status::invalid_argument("missing tenant header"))?
        .to_str()
        .map_err(|_| tonic::Status::invalid_argument("invalid tenant header"))
        .and_then(|tenant| {
            if tenant.is_empty() {
                Err(tonic::Status::invalid_argument("invalid tenant header"))
            } else {
                Ok(tenant)
            }
        })
}
