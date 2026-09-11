use super::*;

/// A request that came through no listener carries neither a principal nor a
/// peer address. While the service reads a credentials file, it must be
/// refused: serving it as unauthenticated would let a router that the service
/// serves without `serve_router` skip the credentials check. With no
/// credentials file, or no service audit context at all, as in an in-process
/// test router, it stays unauthenticated, which is the upstream posture. A
/// principal the authentication layer attached is used as it is.
#[test]
pub(crate) fn a_request_through_no_listener_fails_closed_when_authentication_is_required() {
    let with_audit = |authentication_required| {
        let mut extensions = Extensions::new();
        extensions.insert(ServiceAudit {
            authentication_required,
            ..ServiceAudit::disabled()
        });
        extensions
    };
    let principal_of = |extensions: &Extensions| {
        RequestSecurity::from_extensions(extensions).map(|security| security.principal)
    };

    check!(principal_of(&with_audit(true)) == Err(MissingPrincipal));
    check!(principal_of(&with_audit(false)) == Ok(Principal::Unauthenticated));
    check!(principal_of(&Extensions::new()) == Ok(Principal::Unauthenticated));

    let mut attached = with_audit(true);
    attached.insert(Principal::Unauthenticated);
    check!(principal_of(&attached) == Ok(Principal::Unauthenticated));
}
