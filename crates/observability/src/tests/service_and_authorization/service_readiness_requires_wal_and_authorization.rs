use super::*;

#[test]
pub(crate) fn service_readiness_requires_wal_and_authorization() {
    // A role that registers no precondition is ready: nothing about its
    // startup happens after its listener binds.
    assert2::assert!(RoleReadiness::new().is_ready());

    let readiness = RoleReadiness::new();
    let wal_tail = readiness.gate("wal-tail");
    let authorization = readiness.gate("query-authorization");
    assert2::assert!(!readiness.is_ready());
    assert2::assert!(readiness.pending() == vec!["wal-tail", "query-authorization"]);

    wal_tail.mark_ready();
    assert2::assert!(!readiness.is_ready());
    assert2::assert!(readiness.pending() == vec!["query-authorization"]);

    authorization.mark_ready();
    assert2::assert!(readiness.is_ready());

    // A background task that loses what it had takes the role out of
    // rotation again, without asking for a restart.
    wal_tail.mark_unready();
    assert2::assert!(!readiness.is_ready());
    assert2::assert!(readiness.pending() == vec!["wal-tail"]);
}
