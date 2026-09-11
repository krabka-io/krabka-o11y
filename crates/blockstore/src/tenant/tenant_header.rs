/// The HTTP header, and the gRPC metadata key, that names a request's tenant.
///
/// HTTP header names are case-insensitive, and gRPC metadata keys are
/// lowercase, so this lowercase spelling matches both.
pub const TENANT_HEADER: &str = "x-scope-orgid";
