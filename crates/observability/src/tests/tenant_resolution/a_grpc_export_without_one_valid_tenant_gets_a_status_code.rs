use super::*;

struct GrpcTenantCase {
    name: &'static str,
    value: Option<&'static str>,
    code: tonic::Code,
    message: &'static str,
}

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
        GrpcTenantCase {
            name: "absent",
            value: None,
            code: tonic::Code::Unauthenticated,
            message: "no org id",
        },
        GrpcTenantCase {
            name: "empty",
            value: Some(""),
            code: tonic::Code::Unauthenticated,
            message: "no org id",
        },
        GrpcTenantCase {
            name: "separator",
            value: Some("a/b"),
            code: tonic::Code::InvalidArgument,
            message: "tenant ID 'a/b' contains unsupported character '/'",
        },
        GrpcTenantCase {
            name: "two tenants",
            value: Some("a|b"),
            code: tonic::Code::InvalidArgument,
            message: "multiple org IDs present",
        },
    ];

    for case in refused {
        let status = grpc_tenant(&metadata(case.value)).expect_err("the export is refused");
        check!(
            status.code() == case.code && status.message() == case.message,
            "{}",
            case.name
        );
    }
    check!(
        grpc_tenant(&metadata(Some("tenant-a"))).ok()
            == Some(TenantId::new("tenant-a").expect("a valid tenant id"))
    );
}
