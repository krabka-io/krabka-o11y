use super::*;

pub fn router(state: Arc<DistributorState>) -> Router {
    // Connect routes are built through `MakeServiceBuilder` (not the convenience
    // `build_connect()`) so we can attach a receive-size limit while still
    // applying the same defaults `build_connect()` would: the `ConnectLayer`
    // (protocol detection + per-request `ConnectContext`, without which proto
    // Connect clients like Alloy's `pyroscope.write` / OTLP exporters get
    // `application/json` responses and reject them) plus default gzip
    // decompression. The `receive_max_bytes` cap rejects oversized Connect
    // bodies (via `Content-Length`) before decompression, mirroring the raw
    // doors' body limit. See the matching fix in the querier router.
    let connect_limits =
        MessageLimits::new().receive_max_bytes(state.max_decompressed.bytes_usize());
    let push_router = pb::push::v1::pusher_service_connect::PusherServiceBuilder::<()>::new()
        .push(push_handler)
        .build();
    let push = MakeServiceBuilder::new()
        .message_limits(connect_limits)
        .add_router(push_router)
        .build();
    let otlp_router =
        pb::otlp_profiles::profiles_service_connect::ProfilesServiceBuilder::<()>::new()
            .export(export_handler)
            .build();
    let otlp = MakeServiceBuilder::new()
        .message_limits(connect_limits)
        .add_router(otlp_router)
        .build();

    Router::new()
        .route("/ingest", post(ingest_handler))
        .route("/v1development/profiles", post(otlp_http_handler))
        // Cap raw request bodies at the same limit as Connect and decompression.
        // This bounds memory before the body is buffered and is kept consistent
        // with the per-request gunzip cap (`max_decompressed`).
        .layer(DefaultBodyLimit::max(state.max_decompressed.bytes_usize()))
        .merge(push)
        .merge(otlp)
        .layer(Extension(state))
}
