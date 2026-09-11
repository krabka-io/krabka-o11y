use super::*;

/// Cancelling a delete request removes the one request that matches BOTH
/// the tenant and the id. Two tenants can hold the same id -- the counter
/// is per store, but the ids are handed out per tenant view -- so a cancel
/// that matched on either alone would take a request belonging to someone
/// else.
#[test]
pub(crate) fn cancelling_a_delete_request_takes_only_that_tenant_s() {
    let request = |tenant: &str, request_id: &str| super::super::prelude::CompactorDeleteRequest {
        tenant: tenant.to_string(),
        request_id: request_id.to_string(),
        query: r#"{app="api"}"#.to_string(),
        start_time: 0,
        end_time: 1,
        status: "received".to_string(),
        created_at: 0,
    };
    let state = super::super::prelude::CompactorDeleteState {
        delete_requests: super::super::prelude::SharedLogDeleteRequests::default(),
        query_authorizer: Arc::new(super::super::prelude::AllowAllQueryAuthorizer),
    };
    state
        .delete_requests
        .inner
        .lock()
        .expect("the delete state lock is held")
        .requests = vec![
        request("tenant-a", "delete-1"),
        request("tenant-b", "delete-1"),
        request("tenant-a", "delete-2"),
    ];

    let tenant = super::super::prelude::TenantId::new("tenant-a").expect("a valid tenant id");
    super::super::prelude::execute_cancel_delete_request(
        &state,
        &super::super::prelude::RequestSecurity::unauthenticated(),
        &tenant,
        Some("request_id=delete-1"),
    )
    .expect("the cancel succeeds");

    let left = state
        .delete_requests
        .inner
        .lock()
        .expect("the delete state lock is held")
        .requests
        .iter()
        .map(|request| (request.tenant.clone(), request.request_id.clone()))
        .collect::<Vec<_>>();
    check!(
        left == vec![
            ("tenant-b".to_string(), "delete-1".to_string()),
            ("tenant-a".to_string(), "delete-2".to_string()),
        ],
        "got {left:?}"
    );
}
