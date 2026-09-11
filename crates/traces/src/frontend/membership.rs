//! Who the queriers are, and which of them can answer right now.
//!
//! The frontend used to hold a `Vec<String>` parsed once from `--querier-url`
//! and index it with an atomic counter. That list cannot express a querier
//! that has just started, and it cannot express one that has died: an address
//! that is down keeps taking every Nth job, and an address that was added is
//! never dialled until an operator edits a flag and restarts. Neither reports
//! anything, which is the shape this milestone exists to remove.
//!
//! Membership here is derived from two things the stack already runs:
//!
//! * **DNS.** Every refresh re-resolves the configured endpoints, so the pods
//!   behind a headless Service are the membership rather than a snapshot of it
//!   taken at boot.
//! * **`/ready`.** Every Krabka role serves it, and
//!   `krabka_observability::RoleReadiness` makes it a report of real startup
//!   state: 200 when every gate is met, 503 naming the gates that are not. A
//!   querier that is up but not caught up is therefore distinguishable from
//!   one that can answer, which is exactly the distinction a fan-out needs.
//!
//! What was rejected, and why:
//!
//! * **Broker group membership.** The broker does track consumer-group
//!   membership, and the queriers that run an embedded live-store are in one.
//!   But a querier configured with `--querier-live-store-url` joins no group
//!   at all, so the broker's view is incomplete for the thing being asked; and
//!   reading it would make the query-frontend the first library or binary in
//!   the workspace to depend on `krabka-broker`, which is a dev-dependency of
//!   two crates and nothing else.
//! * **A hash ring over memberlist or Consul**, which is what dskit gives
//!   Loki, Mimir and Tempo. It is the right answer at their scale and it is a
//!   whole gossip protocol, a new failure domain, and a new operational
//!   surface. The only ring in this workspace today is a static compatibility
//!   page. `/ready` plus DNS answers the same question with nothing new
//!   running.
//! * **DNS alone.** Re-resolution finds an address; it does not say whether
//!   the process behind it can answer. A pod that is listening but has not
//!   loaded its index is in DNS and is not able to serve.

use std::{collections::BTreeSet, sync::Arc, time::Duration};

use arc_swap::ArcSwap;
use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use crate::frontend::backend::BackendError;

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    /// A probe with a canned verdict per address, and no address at all for
    /// one it was never told about.
    struct StubProbe(Vec<(String, QuerierHealth)>);

    #[async_trait]
    impl ReadinessProbe for StubProbe {
        async fn probe(&self, addr: &str) -> QuerierHealth {
            self.0.iter().find(|(a, _)| a == addr).map_or_else(
                || QuerierHealth::Unreachable {
                    error: "no stub".to_string(),
                },
                |(_, health)| health.clone(),
            )
        }
    }

    #[test]
    fn only_ready_members_take_jobs_and_the_rest_say_why() {
        let membership = Membership::new(
            vec![
                QuerierMember::ready("b:1"),
                QuerierMember {
                    addr: "a:1".to_string(),
                    health: QuerierHealth::NotReady {
                        pending: "trace-index".to_string(),
                    },
                },
                QuerierMember {
                    addr: "c:1".to_string(),
                    health: QuerierHealth::Unreachable {
                        error: "connection refused".to_string(),
                    },
                },
            ],
            7,
        );

        check!(membership.ready_addrs() == vec!["b:1"]);
        check!(membership.ready_count() == 1);
        check!(membership.generation() == 7);
        // Address order, not probe order, so assignment is reproducible.
        check!(
            membership
                .members()
                .iter()
                .map(|m| m.addr.as_str())
                .collect::<Vec<_>>()
                == vec!["a:1", "b:1", "c:1"]
        );
        let warnings = membership.exclusion_warnings();
        check!(warnings.len() == 2);
        check!(warnings[0].contains("a:1") && warnings[0].contains("not ready: trace-index"));
        check!(
            warnings[1].contains("c:1") && warnings[1].contains("unreachable: connection refused")
        );
    }

    #[test]
    fn a_fixed_view_is_every_address_ready_and_a_publish_advances_the_generation() {
        let view = MembershipView::fixed(["q1:3200", "q2:3200"]);
        check!(view.load().ready_addrs() == vec!["q1:3200", "q2:3200"]);
        check!(view.load().generation() == 0);

        view.publish(vec![QuerierMember::ready("q1:3200")]);
        check!(view.load().ready_addrs() == vec!["q1:3200"]);
        check!(view.load().generation() == 1);
    }

    #[test]
    fn an_empty_view_offers_nobody_to_assign_to() {
        check!(MembershipView::empty().load().ready_count() == 0);
    }

    #[tokio::test]
    async fn a_refresh_probes_every_resolved_address_and_keeps_the_verdicts() {
        // Loopback resolves to itself, so the endpoint and the member address
        // agree and the stub can be keyed on it.
        let probe = StubProbe(vec![
            ("127.0.0.1:1".to_string(), QuerierHealth::Ready),
            (
                "127.0.0.1:2".to_string(),
                QuerierHealth::NotReady {
                    pending: "live-store".to_string(),
                },
            ),
        ]);
        let members = refresh_membership(
            &["127.0.0.1:1".to_string(), "127.0.0.1:2".to_string()],
            &probe,
        )
        .await;
        let snapshot = Membership::new(members, 1);
        check!(snapshot.ready_addrs() == vec!["127.0.0.1:1"]);
        check!(snapshot.exclusion_warnings().len() == 1);
    }

    #[tokio::test]
    async fn an_endpoint_that_does_not_resolve_stays_a_member_so_it_can_be_reported() {
        // A name with no records must not quietly vanish from the membership:
        // a member nobody knows about earns no warning.
        let resolved = resolve_endpoints(&["no-such-host.invalid:3200".to_string()]).await;
        check!(resolved == vec!["no-such-host.invalid:3200".to_string()]);
    }

    #[tokio::test]
    async fn the_probe_reads_the_gate_names_out_of_a_role_readiness_503() {
        use axum::{Router, http::StatusCode, routing::get};

        let app = Router::new()
            .route("/ready", get(|| async { "ready\n" }))
            .route(
                "/starting",
                get(|| async {
                    (
                        StatusCode::SERVICE_UNAVAILABLE,
                        "not ready: trace-index, live-store\n",
                    )
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let probe = HttpReadinessProbe::new(Duration::from_secs(5)).unwrap();
        check!(probe.probe(&addr.to_string()).await == QuerierHealth::Ready);

        // A port with nothing behind it is unreachable, not unready.
        let dead = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let dead_addr = dead.local_addr().unwrap();
        drop(dead);
        let health = probe.probe(&dead_addr.to_string()).await;
        check!(matches!(health, QuerierHealth::Unreachable { .. }));
    }

    #[test]
    fn a_503_body_keeps_its_gate_names_and_an_empty_one_admits_it_has_none() {
        check!(super::http_readiness_probe::pending_gates("not ready: a, b\n") == "a, b");
        check!(super::http_readiness_probe::pending_gates("") == "unnamed gate");
        check!(super::http_readiness_probe::pending_gates("service unavailable") == "unnamed gate");
    }
}

mod http_readiness_probe;
mod membership_snapshot;
mod membership_view;
mod querier_health;
mod querier_member;
mod readiness_probe;
mod refresh_membership;
mod resolve_endpoints;
mod run_membership_refresh;

pub use http_readiness_probe::HttpReadinessProbe;
pub use membership_snapshot::Membership;
pub use membership_view::MembershipView;
pub use querier_health::QuerierHealth;
pub use querier_member::QuerierMember;
pub use readiness_probe::ReadinessProbe;
pub use refresh_membership::refresh_membership;
pub use resolve_endpoints::resolve_endpoints;
pub use run_membership_refresh::run_membership_refresh;
