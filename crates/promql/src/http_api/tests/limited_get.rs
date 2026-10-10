use super::*;

/// A response's status and parsed JSON body.
pub(crate) struct JsonReply {
    pub(crate) status: StatusCode,
    pub(crate) body: serde_json::Value,
}

/// Sends a `tenant-a` GET of `uri` to a router over `store` under the
/// per-tenant query `limits`, and returns the reply.
pub(crate) async fn limited_get(
    store: InMemoryMetricStore,
    limits: Limits,
    uri: &str,
) -> JsonReply {
    let state = Arc::new(
        PrometheusApiState::new(Arc::new(store), EngineOpts::default())
            .with_query_limits(OverridesProvider::new(limits)),
    );
    let (status, body) = annotated_query_body(state, uri).await;
    JsonReply { status, body }
}

/// Checks a `422` Prometheus `execution` error whose message `message_matches`
/// accepts. `context` labels each failure.
pub(crate) fn assert_execution_error(
    JsonReply { status, body }: &JsonReply,
    message_matches: impl Fn(&str) -> bool,
    context: &str,
) {
    assert2::assert!(*status == StatusCode::UNPROCESSABLE_ENTITY, "{context}");
    assert2::assert!(body["status"].as_str() == Some("error"), "{context}");
    assert2::assert!(body["errorType"].as_str() == Some("execution"), "{context}");
    assert2::assert!(
        body["error"].as_str().is_some_and(&message_matches),
        "{context}"
    );
}
