use super::*;

/// A `tenant-a` request for `query` at the single instant 0, split into
/// 60s ranges and spread over two shards.
pub(crate) fn two_shard_instant_request(query: &str) -> FrontendRangeRequest {
    FrontendRangeRequest {
        tenant: tenant_id("tenant-a"),
        query: query.into(),
        start_ms: 0,
        end_ms: 0,
        step: millis(60_000),
        admission_limits: krabka_query_frontend::AdmissionLimits::default(),
        opts: QueryFrontendOptions {
            split_interval: millis(60_000),
            shard_count: 2,
        },
    }
}

/// A one-shard `tenant-a` request for `up` from 0 to 360s at a 60s step,
/// split into 120s ranges.
pub(crate) fn split_up_request() -> FrontendRangeRequest {
    FrontendRangeRequest {
        tenant: tenant_id("tenant-a"),
        query: "up".into(),
        start_ms: 0,
        end_ms: 360_000,
        step: millis(60_000),
        admission_limits: krabka_query_frontend::AdmissionLimits::default(),
        opts: QueryFrontendOptions {
            split_interval: millis(120_000),
            shard_count: 1,
        },
    }
}
