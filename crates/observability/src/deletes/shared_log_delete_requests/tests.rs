use std::{sync::mpsc, thread};

use assert2::assert;

use super::*;
use crate::{
    AllowAllQueryAuthorizer, Bytes, CompactorDeleteState, RequestSecurity, TenantId,
    deletes_api::{
        execute_cancel_delete_request, execute_create_delete_request, execute_list_delete_requests,
    },
};

#[test]
fn a_refresh_cannot_restore_an_acknowledged_cancellation() {
    let directory = tempfile::tempdir().unwrap();
    let state = CompactorDeleteState {
        delete_requests: SharedLogDeleteRequests::from_data_root(directory.path()).unwrap(),
        query_authorizer: Arc::new(AllowAllQueryAuthorizer),
    };
    let tenant = TenantId::new("tenant-a").unwrap();
    execute_create_delete_request(
        &state,
        &RequestSecurity::unauthenticated(),
        &tenant,
        Some(r#"query={job="api"}&start=1&end=2"#),
        &Bytes::new(),
    )
    .unwrap();

    let refresh = state.delete_requests.clone();
    let (read_tx, read_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    let reader = thread::spawn(move || {
        refresh
            .refresh_from(|path| {
                let snapshot = read_log_delete_requests(path)?;
                read_tx.send(()).unwrap();
                resume_rx.recv().unwrap();
                Ok(snapshot)
            })
            .unwrap();
    });
    read_rx.recv().unwrap();

    let reader_holds_lock = state.delete_requests.inner.try_lock().is_err();
    let cancel_state = state.clone();
    let cancel_tenant = tenant.clone();
    let cancel = move || {
        execute_cancel_delete_request(
            &cancel_state,
            &RequestSecurity::unauthenticated(),
            &cancel_tenant,
            Some("request_id=delete-1"),
        )
        .unwrap();
    };
    // A reader that holds the lock finishes first. Otherwise, let the actual
    // cancel API acknowledge its write before the stale read is installed.
    if reader_holds_lock {
        let writer = thread::spawn(cancel);
        resume_tx.send(()).unwrap();
        reader.join().unwrap();
        writer.join().unwrap();
    } else {
        cancel();
        resume_tx.send(()).unwrap();
        reader.join().unwrap();
    }

    let requests = execute_list_delete_requests(&state, &tenant, None).unwrap();
    assert!(serde_json::to_value(requests).unwrap() == serde_json::json!([]));
    let expected = serde_json::json!({"next_id": 1, "requests": []});
    assert!(
        serde_json::to_value(&*state.delete_requests.inner.lock().unwrap()).unwrap() == expected
    );
    let persisted = SharedLogDeleteRequests::from_data_root(directory.path()).unwrap();
    assert!(serde_json::to_value(&*persisted.inner.lock().unwrap()).unwrap() == expected);
}
