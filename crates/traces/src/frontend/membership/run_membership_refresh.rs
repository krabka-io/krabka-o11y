use super::{
    Arc, CancellationToken, Duration, MembershipView, ReadinessGate, ReadinessProbe,
    mark_querier_membership_gate, refresh_membership,
};

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
