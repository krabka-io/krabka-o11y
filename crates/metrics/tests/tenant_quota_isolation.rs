//! Slice 8 hardening: per-tenant ingestion-quota isolation in the distributor.
//!
//! This test boots the distributor HTTP router on a real ephemeral
//! `127.0.0.1:0` socket, with an in-memory `WalSink` and per-tenant limit
//! overrides. `org-a` gets a TIGHT ingestion-rate quota, and `org-b` gets a
//! generous one. The test pushes `remote_write` to `org-a` until it returns
//! HTTP 429, then pushes the SAME load to `org-b` and asserts that it still
//! succeeds. That proves the token bucket is per-tenant and not global.

use assert2::assert;
use krabka_metrics::OverridesProvider;

#[path = "support/overrides_distributor.rs"]
mod overrides_distributor;
#[path = "support/recording_sink.rs"]
mod recording_sink;
#[path = "support/up_remote_write.rs"]
mod up_remote_write;

use self::{overrides_distributor::OverridesDistributor, up_remote_write::remote_write_v1_body};

const ORG_A: &str = "org-a";
const ORG_B: &str = "org-b";

/// Per-tenant overrides. A rate limit holds org-a to a single sample of burst,
/// and org-b is unlimited in practice. An unlisted tenant falls back to the
/// defaults.
fn tenant_overrides() -> OverridesProvider {
    let yaml = r#"
overrides:
  org-a:
    ingestion_rate: "1/s"
    ingestion_burst_size: 1
  org-b:
    ingestion_rate: "1000000/s"
    ingestion_burst_size: 1000000
"#;
    OverridesProvider::from_yaml(yaml).expect("parse tenant overrides")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn per_tenant_quota_is_isolated() {
    let distributor = OverridesDistributor::boot(tenant_overrides()).await;
    let client = reqwest::Client::new();

    // Drive org-a until its tight bucket rejects with 429. Bounded loop so a
    // regression (global / never-tripping quota) fails fast instead of hanging.
    let mut org_a_throttled = false;
    let mut org_a_successes = 0usize;
    for _ in 0..50 {
        let status = distributor
            .push(&client, ORG_A, remote_write_v1_body())
            .await;
        if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            org_a_throttled = true;
            break;
        }
        assert!(
            status.is_success(),
            "org-a push before throttle expected success, got {status}"
        );
        org_a_successes += 1;
    }
    assert!(
        org_a_throttled,
        "org-a was never rate-limited within 50 pushes (after {org_a_successes} accepted) \
         — its tight quota is not being enforced"
    );

    // Same load under org-b's own tenant header must still be accepted: the
    // token bucket is per-tenant, so org-a draining its bucket cannot starve
    // org-b. A global bucket would already be empty here and return 429.
    let appends_before_b = distributor.sink.len();
    for index in 0..10 {
        let status = distributor
            .push(&client, ORG_B, remote_write_v1_body())
            .await;
        assert!(
            status.is_success(),
            "org-b push #{index} should succeed under its own generous quota, got {status} \
             — quota is leaking across tenants (global, not per-tenant)"
        );
    }

    // org-b's accepted pushes must have reached the WAL sink.
    assert!(
        distributor.sink.len() >= appends_before_b + 10,
        "expected org-b's 10 pushes to append to the WAL sink"
    );

    // Sanity: org-a really is still throttled (its bucket stays drained) while
    // org-b keeps succeeding — confirms the two buckets are independent.
    assert!(
        distributor
            .push(&client, ORG_A, remote_write_v1_body())
            .await
            == reqwest::StatusCode::TOO_MANY_REQUESTS,
        "org-a should remain throttled"
    );
    assert!(
        distributor
            .push(&client, ORG_B, remote_write_v1_body())
            .await
            .is_success(),
        "org-b should remain unaffected by org-a's throttling"
    );
}
