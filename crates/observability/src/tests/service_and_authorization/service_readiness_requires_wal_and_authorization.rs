use super::*;

#[test]
pub(crate) fn service_readiness_requires_wal_and_authorization() {
    assert2::assert!(ServiceReadiness::ready().is_ready());

    let readiness = ServiceReadiness::deferred_querier();
    assert2::assert!(!readiness.is_ready());
    readiness.wal_connected.store(true, AtomicOrdering::SeqCst);
    assert2::assert!(!readiness.is_ready());
    readiness.wal_connected.store(false, AtomicOrdering::SeqCst);
    readiness
        .authorization_connected
        .store(true, AtomicOrdering::SeqCst);
    assert2::assert!(!readiness.is_ready());
    readiness.wal_connected.store(true, AtomicOrdering::SeqCst);
    assert2::assert!(readiness.is_ready());
}
