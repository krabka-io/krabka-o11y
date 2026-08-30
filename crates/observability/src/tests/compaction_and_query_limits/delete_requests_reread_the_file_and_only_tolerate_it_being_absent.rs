use super::*;

/// Only a *missing* delete-request file reads as "no requests"; every
/// other IO failure is an error. `refresh` then has to actually re-read
/// that file -- a compactor that never picks up a request written by
/// another process deletes nothing.
#[test]
pub(crate) fn delete_requests_reread_the_file_and_only_tolerate_it_being_absent() {
    let dir = tempfile::tempdir().expect("a temp dir");
    let store = SharedLogDeleteRequests::from_data_root(dir.path())
        .expect("an absent file is not an error");
    check!(store.inner.lock().expect("not poisoned").next_id == 0);

    // Another process writes a request while this one holds the store.
    std::fs::write(
            log_delete_requests_path(dir.path()),
            r#"{"next_id":7,"requests":[{"tenant":"tenant-a","request_id":"r-1","query":"{job=\"api\"}","start_time":1,"end_time":2,"status":"received","created_at":3}]}"#,
        )
        .expect("write the request file");
    store.refresh().expect("the written file is readable");
    let inner = store.inner.lock().expect("not poisoned");
    check!(inner.next_id == 7);
    check!(inner.requests.len() == 1);
    check!(inner.requests[0].request_id == "r-1");
    drop(inner);

    // A directory in the file's place fails to read, and is not NotFound.
    let as_directory = dir.path().join("a-directory");
    std::fs::create_dir(&as_directory).expect("create the directory");
    let error = read_log_delete_requests(&as_directory)
        .expect_err("a directory is not a readable request file");
    check!(matches!(error, LogDeleteRequestStoreError::Io { .. }));
}
