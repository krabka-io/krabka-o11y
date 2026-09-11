use super::{Extension, Router, ServiceAudit};

/// `router`, with `audit` in the extensions of every request that it serves.
///
/// Call it on the finished router. A route that is merged in later does not
/// get the extension.
pub(crate) fn with_service_audit(router: Router, audit: ServiceAudit) -> Router {
    router.layer(Extension(audit))
}
