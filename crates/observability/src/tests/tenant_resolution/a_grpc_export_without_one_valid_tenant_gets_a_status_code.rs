use super::*;

/// Loki has no OTLP gRPC door, so these codes follow the HTTP push: 401
/// becomes `UNAUTHENTICATED` and 400 becomes `INVALID_ARGUMENT`, with the
/// same message.
#[test]
pub(crate) fn a_grpc_export_without_one_valid_tenant_gets_a_status_code() {
    let metadata = |value: Option<&str>| {
        let mut metadata = tonic::metadata::MetadataMap::new();
        if let Some(value) = value {
            metadata.insert("x-scope-orgid", value.parse().expect("a metadata value"));
        }
        metadata
    };
    let refused = [
        ("absent", None, tonic::Code::Unauthenticated, "no org id"),
        ("empty", Some(""), tonic::Code::Unauthenticated, "no org id"),
        (
            "separator",
            Some("a/b"),
            tonic::Code::InvalidArgument,
            "tenant ID 'a/b' contains unsupported character '/'",
        ),
        (
            "two tenants",
            Some("a|b"),
            tonic::Code::InvalidArgument,
            "multiple org IDs present",
        ),
    ];

    for (name, value, code, message) in refused {
        let status = grpc_tenant(&metadata(value)).expect_err("the export is refused");
        check!(
            (status.code(), status.message()) == (code, message),
            "{name}"
        );
    }
    check!(
        grpc_tenant(&metadata(Some("tenant-a"))).ok()
            == Some(TenantId::new("tenant-a").expect("a valid tenant id"))
    );
}
