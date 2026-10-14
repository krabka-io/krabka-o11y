#[tokio::test]
pub(crate) async fn querier_state_with_request_tenant_index_tolerates_absent_manifest() {
    super::absent_dynamic_index_is_an_empty_block_index(false).await;
}
