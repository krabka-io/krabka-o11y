use super::*;

/// A delete request records its creation time in Unix *seconds*, the unit
/// Loki's own API reports. Dividing rather than taking a remainder is what
/// makes it a date at all -- a remainder is always under one second.
#[test]
pub(crate) fn a_created_delete_request_is_stamped_in_whole_seconds() {
    let dir = tempfile::tempdir().expect("a temp dir");
    let state = CompactorDeleteState {
        delete_requests: SharedLogDeleteRequests::from_data_root(dir.path())
            .expect("an absent file is not an error"),
    };
    let mut headers = HeaderMap::new();
    headers.insert("X-Scope-OrgID", "tenant-a".parse().expect("a header value"));

    execute_create_delete_request(
        &state,
        &headers,
        Some(r#"query={job="api"}&start=1&end=2"#),
        &Bytes::new(),
    )
    .expect("the request is accepted");

    let requests = state
        .delete_requests
        .inner
        .lock()
        .expect("compactor delete state poisoned");
    check!(requests.requests.len() == 1);
    let created_at = requests.requests[0].created_at;
    // Any wall clock this code runs on is past 2020, and a sub-second
    // remainder can never reach that.
    check!(created_at > 1_600_000_000, "created_at was {created_at}");
}
