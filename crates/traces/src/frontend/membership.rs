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

#![cfg_attr(test, allow(clippy::items_after_test_module))]

use std::{collections::BTreeSet, sync::Arc, time::Duration};

use arc_swap::ArcSwap;
use async_trait::async_trait;
use krabka_observability::{ReadinessGate, server_security::InternalClient};
use tokio_util::sync::CancellationToken;

use crate::frontend::{QuerierScheme, backend::BackendError, http_backend::internal_http_client};

/// One consistent view of the querier pool, taken at an instant.
///
/// A query plans, fans out and merges against a single snapshot. Planning
/// against one membership and collecting against another is how a fan-out
/// loses a shard without noticing: the job went to a querier that the
/// collecting view no longer contains, so nothing is left to attribute the
/// gap to. The `generation` makes a change across a query detectable, and
/// [`QueryFrontend::search`](crate::frontend::QueryFrontend::search) reports
/// one that happened while a live shard was in flight.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Membership {
    members: Vec<QuerierMember>,
    generation: u64,
}

impl Membership {
    /// A snapshot of `members`, ordered by address so assignment is
    /// reproducible whatever order DNS and the probes answered in.
    #[must_use]
    pub fn new(mut members: Vec<QuerierMember>, generation: u64) -> Self {
        members.sort_by(|a, b| a.addr.cmp(&b.addr));
        members.dedup_by(|a, b| a.addr == b.addr);
        Self {
            members,
            generation,
        }
    }

    /// Which refresh produced this snapshot. It only ever increases.
    #[must_use]
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// Every known querier, ready or not.
    #[must_use]
    pub fn members(&self) -> &[QuerierMember] {
        &self.members
    }

    /// The addresses a job may be assigned to, in address order.
    #[must_use]
    pub fn ready_addrs(&self) -> Vec<&str> {
        self.members
            .iter()
            .filter(|m| m.is_ready())
            .map(|m| m.addr.as_str())
            .collect()
    }

    /// How many queriers may take a job.
    #[must_use]
    pub fn ready_count(&self) -> usize {
        self.members.iter().filter(|m| m.is_ready()).count()
    }

    /// One warning line per querier left out of the fan-out.
    #[must_use]
    pub fn exclusion_warnings(&self) -> Vec<String> {
        self.members
            .iter()
            .filter_map(QuerierMember::warning)
            .collect()
    }
}

/// The shared, swappable membership the refresh loop writes and every query
/// reads.
///
/// Cloning it shares the same cell. A reader takes an `Arc<Membership>` and
/// holds it for the whole query, so a refresh mid-query replaces the cell
/// without moving the ground under a fan-out that is already in flight.
#[derive(Clone)]
pub struct MembershipView {
    inner: Arc<ArcSwap<Membership>>,
}

impl MembershipView {
    /// A view that knows no queriers yet, as a frontend does before its first
    /// probe. Every query against it fails rather than answering from nothing.
    #[must_use]
    pub fn empty() -> Self {
        Self {
            inner: Arc::new(ArcSwap::from_pointee(Membership::default())),
        }
    }

    /// A view whose members never change and are all ready.
    ///
    /// This is the in-process case: a test, or any caller that has already
    /// decided who the queriers are.
    #[must_use]
    pub fn fixed<I, S>(addrs: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let members = addrs.into_iter().map(QuerierMember::ready).collect();
        Self {
            inner: Arc::new(ArcSwap::from_pointee(Membership::new(members, 0))),
        }
    }

    /// The current snapshot. Hold the returned handle for a whole query.
    #[must_use]
    pub fn load(&self) -> Arc<Membership> {
        self.inner.load_full()
    }

    /// Replaces the snapshot with `members`, at the next generation.
    pub fn publish(&self, members: Vec<QuerierMember>) {
        let generation = self.inner.load().generation().wrapping_add(1);
        self.inner
            .store(Arc::new(Membership::new(members, generation)));
    }
}

impl Default for MembershipView {
    fn default() -> Self {
        Self::empty()
    }
}

/// What a querier's last readiness probe said.
///
/// The three cases are distinct on purpose. A querier that answers `/ready`
/// with a 503 naming its unmet gates is starting up or has fallen behind, and
/// will come back. One that does not answer at all is gone, or the network to
/// it is. Both are out of the fan-out, and both say why in the response
/// warnings, which is the difference between an ejection and a silent loss.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum QuerierHealth {
    /// `/ready` answered 2xx: every gate the role registered is met.
    Ready,
    /// `/ready` answered 503 and named the gates still unmet, as
    /// `krabka_observability::RoleReadiness` renders them.
    NotReady {
        /// The comma-separated gate names the querier reported, or
        /// `unnamed gate` when it reported none.
        pending: String,
    },
    /// The probe did not complete, or answered something other than 2xx/503.
    Unreachable {
        /// The transport or status detail, for the operator reading a warning.
        error: String,
    },
}

impl QuerierHealth {
    /// Whether this querier may take a job.
    #[must_use]
    pub fn is_ready(&self) -> bool {
        matches!(self, Self::Ready)
    }

    /// Why this querier is out of the fan-out, or `None` when it is in it.
    #[must_use]
    pub fn reason(&self) -> Option<String> {
        match self {
            Self::Ready => None,
            Self::NotReady { pending } => Some(format!("not ready: {pending}")),
            Self::Unreachable { error } => Some(format!("unreachable: {error}")),
        }
    }
}

/// One querier the frontend knows about, and what its last probe said.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QuerierMember {
    /// The `host:port` the frontend dials, after DNS re-resolution.
    pub addr: String,
    /// The result of the last readiness probe.
    pub health: QuerierHealth,
}

impl QuerierMember {
    /// A member whose last probe succeeded.
    #[must_use]
    pub fn ready(addr: impl Into<String>) -> Self {
        Self {
            addr: addr.into(),
            health: QuerierHealth::Ready,
        }
    }

    /// Whether this member may take a job.
    #[must_use]
    pub fn is_ready(&self) -> bool {
        self.health.is_ready()
    }

    /// The response warning this member's exclusion earns, or `None` when it
    /// is in the fan-out.
    ///
    /// The wording names the address and the reason, because an operator
    /// reading it from a Grafana panel has neither.
    #[must_use]
    pub fn warning(&self) -> Option<String> {
        self.health.reason().map(|reason| {
            format!(
                "querier {} was excluded from the fan-out ({reason}); data only it holds is missing from this result",
                self.addr
            )
        })
    }
}

/// How the frontend asks one querier whether it can answer.
///
/// The production implementation is [`HttpReadinessProbe`](HttpReadinessProbe),
/// which reads the querier's own `/ready`. The trait exists so a test can put
/// a querier into any health without needing a process that is genuinely in
/// it.
#[async_trait]
pub trait ReadinessProbe: Send + Sync {
    /// Ask `addr` whether it is ready. This never fails: a probe that cannot
    /// complete is itself the answer.
    async fn probe(&self, addr: &str) -> QuerierHealth;
}

/// Re-resolve the configured querier endpoints into the addresses behind them
/// right now.
///
/// A flag holds names, not instances. `querier.traces.svc:3200` is one
/// headless Service in front of however many querier pods exist this minute,
/// and resolving it once at startup is what makes a scaled-up querier
/// unreachable until someone restarts the frontend. Resolving it every refresh
/// is what makes the new pod a member.
///
/// An endpoint that does not resolve is kept as itself rather than dropped.
/// Dropping it would remove it from the membership, and a querier that is
/// absent from the membership earns no warning -- exactly the silence this
/// exists to remove. Kept, it fails its probe and says so.
pub async fn resolve_endpoints(endpoints: &[String]) -> Vec<String> {
    let mut resolved = BTreeSet::new();
    for endpoint in endpoints {
        match tokio::net::lookup_host(endpoint.as_str()).await {
            Ok(addrs) => {
                let before = resolved.len();
                for addr in addrs {
                    resolved.insert(addr.to_string());
                }
                if resolved.len() == before {
                    resolved.insert(endpoint.clone());
                }
            }
            Err(error) => {
                tracing::warn!(%endpoint, %error, "querier endpoint did not resolve");
                resolved.insert(endpoint.clone());
            }
        }
    }
    resolved.into_iter().collect()
}

/// Re-resolve and re-probe the querier pool on `interval` until `shutdown`.
///
/// Register it with `krabka_observability::SupervisedTasks`, never with a bare
/// `tokio::spawn`. If this loop stops, the frontend keeps fanning out over
/// whichever membership it last published, which goes stale silently. That is
/// the precise failure the loop exists to prevent, restored by its own death.
///
/// The first tick fires immediately, so a caller that spawns this and serves
/// straight away is at most one probe behind rather than one interval behind.
///
/// `membership_gate` is the frontend's own `querier-membership` gate. Each
/// refresh moves it: up while some querier passes its probe, down while none
/// does.
pub async fn run_membership_refresh(
    view: MembershipView,
    endpoints: Vec<String>,
    probe: Arc<dyn ReadinessProbe>,
    interval: Duration,
    membership_gate: ReadinessGate,
    shutdown: CancellationToken,
) {
    let mut tick = tokio::time::interval(interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            () = shutdown.cancelled() => break,
            _ = tick.tick() => {
                let members = refresh_membership(&endpoints, probe.as_ref()).await;
                let ready = members.iter().filter(|m| m.is_ready()).count();
                let total = members.len();
                if ready < total {
                    for member in members.iter().filter(|m| !m.is_ready()) {
                        tracing::warn!(
                            querier = %member.addr,
                            health = ?member.health,
                            "querier excluded from the query fan-out",
                        );
                    }
                }
                if ready == 0 {
                    tracing::error!(%total, "no querier is ready; queries will fail rather than answer partially");
                }
                mark_querier_membership_gate(&membership_gate, &members);
                view.publish(members);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    /// A probe with a canned verdict per address, and no address at all for
    /// one it was never told about.
    struct StubProbe(Vec<QuerierMember>);

    #[async_trait]
    impl ReadinessProbe for StubProbe {
        async fn probe(&self, addr: &str) -> QuerierHealth {
            self.0
                .iter()
                .find(|member| member.addr == addr)
                .map_or_else(
                    || QuerierHealth::Unreachable {
                        error: "no stub".to_string(),
                    },
                    |member| member.health.clone(),
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
            QuerierMember {
                addr: "127.0.0.1:1".to_string(),
                health: QuerierHealth::Ready,
            },
            QuerierMember {
                addr: "127.0.0.1:2".to_string(),
                health: QuerierHealth::NotReady {
                    pending: "live-store".to_string(),
                },
            },
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

    /// The probe and the role it probes are one pair, so this drives the real
    /// `krabka_observability` handler rather than a hand-written body. A
    /// renderer that changed its wording would leave the probe saying
    /// `unnamed gate`, and a querier that is still loading its index would then
    /// look the same as one that answered 503 for any other reason.
    #[tokio::test]
    async fn the_probe_reads_the_gate_names_out_of_a_role_readiness_503() {
        use krabka_observability::RoleReadiness;

        let readiness = RoleReadiness::new();
        let trace_index = readiness.gate("trace-index");
        let app = krabka_observability::readiness_router(readiness);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let probe = HttpReadinessProbe::new(
            Duration::from_secs(5),
            QuerierScheme::Http,
            &InternalClient::default(),
        )
        .unwrap();
        check!(
            probe.probe(&addr.to_string()).await
                == QuerierHealth::NotReady {
                    pending: "trace-index".to_string(),
                }
        );

        trace_index.mark_ready();
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
        check!(super::pending_gates("not ready: a, b\n") == "a, b");
        check!(super::pending_gates("") == "unnamed gate");
        check!(super::pending_gates("service unavailable") == "unnamed gate");
    }
}

/// Resolve `endpoints` and probe every address behind them, concurrently.
///
/// The result is the member list for the next generation: every querier the
/// names point at, each carrying what its probe said.
pub async fn refresh_membership(
    endpoints: &[String],
    probe: &dyn ReadinessProbe,
) -> Vec<QuerierMember> {
    let addrs = resolve_endpoints(endpoints).await;
    futures::future::join_all(addrs.into_iter().map(|addr| async move {
        let health = probe.probe(&addr).await;
        QuerierMember { addr, health }
    }))
    .await
}

/// Reads a querier's `/ready`, the endpoint every Krabka role already serves.
///
/// `krabka_observability::RoleReadiness` answers 200 when every registered
/// gate is met, and 503 with `not ready: <gate>, <gate>` when some are not.
/// This probe keeps that distinction: a 503 is a querier that will come back,
/// and its gate names go into the response warning verbatim.
pub struct HttpReadinessProbe {
    http: reqwest::Client,
    scheme: QuerierScheme,
}

impl HttpReadinessProbe {
    /// Build the probe client. `timeout` bounds a single probe, and wants to
    /// be far shorter than a query timeout: a probe that hangs holds up the
    /// whole refresh.
    ///
    /// The probe dials `scheme://addr/ready` with `internal_client` applied.
    /// `/ready` needs no credential, but a querier that serves TLS needs the
    /// CA bundle of the internal client to verify.
    ///
    /// # Errors
    /// Returns `BackendError::Transport` when the HTTP client cannot be built.
    pub fn new(
        timeout: Duration,
        scheme: QuerierScheme,
        internal_client: &InternalClient,
    ) -> Result<Self, BackendError> {
        let http = internal_http_client(timeout, internal_client)?;
        Ok(Self { http, scheme })
    }
}

#[async_trait]
impl ReadinessProbe for HttpReadinessProbe {
    async fn probe(&self, addr: &str) -> QuerierHealth {
        let url = format!("{}://{addr}/ready", self.scheme.as_str());
        let resp = match self.http.get(url).send().await {
            Ok(resp) => resp,
            Err(error) => {
                return QuerierHealth::Unreachable {
                    error: error.to_string(),
                };
            }
        };
        let status = resp.status();
        if status.is_success() {
            return QuerierHealth::Ready;
        }
        if status != reqwest::StatusCode::SERVICE_UNAVAILABLE {
            return QuerierHealth::Unreachable {
                error: format!("/ready returned {status}"),
            };
        }
        let body = resp.text().await.unwrap_or_default();
        QuerierHealth::NotReady {
            pending: pending_gates(&body),
        }
    }
}

/// Pull the gate names out of a `RoleReadiness` 503 body.
///
/// The body is `not ready: <names>\n`. Anything else -- another component's
/// 503, an empty body -- still means unready, so it reports an unnamed gate
/// rather than claiming to know which one.
pub(super) fn pending_gates(body: &str) -> String {
    let named = body
        .trim()
        .strip_prefix("not ready:")
        .map_or_else(Default::default, str::trim);
    if named.is_empty() {
        "unnamed gate".to_string()
    } else {
        named.to_string()
    }
}

/// The readiness gate a query-frontend holds while at least one querier can
/// answer.
///
/// The frontend has nowhere to send a job when no querier passes its own
/// `/ready`, and
/// [`QueryFrontend::search`](crate::frontend::QueryFrontend::search) fails
/// every query in that state. The gate makes the frontend say so on its own
/// `/ready`, so an orchestrator routes around it rather than sending it
/// queries it can only refuse.
///
/// The name is the one `/ready` prints: `not ready: querier-membership`.
pub const QUERIER_MEMBERSHIP_GATE: &str = "querier-membership";

/// Moves `gate` to match a freshly probed pool.
///
/// A pool with nobody ready takes the frontend out of rotation, and the first
/// querier to come back puts it in again. The gate therefore goes down as well
/// as up, for the same reason the queriers' own gates do.
pub(crate) fn mark_querier_membership_gate(gate: &ReadinessGate, members: &[QuerierMember]) {
    if members.iter().any(QuerierMember::is_ready) {
        gate.mark_ready();
    } else {
        gate.mark_unready();
    }
}
