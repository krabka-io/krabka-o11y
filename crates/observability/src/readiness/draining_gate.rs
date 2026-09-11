/// The readiness gate a role holds while it is willing to take new writes.
///
/// An operator draining a pod wants the load balancer to stop sending it
/// traffic. Krabka joins no hash ring, so there is nothing to unregister from;
/// what a drain can do, and what `Loki`'s `prepare_shutdown` achieves through
/// the ring, is make the readiness probe fail. `POST /ingester/prepare_shutdown`
/// therefore marks this gate unmet, `/ready` starts answering 503, and the
/// orchestrator routes elsewhere. `DELETE` on the same path puts it back, which
/// is how `Loki` cancels a drain that was started by mistake.
///
/// The name is the one `/ready` prints, so it reads as a sentence: `not ready:
/// accepting-writes`.
pub const DRAINING_GATE: &str = "accepting-writes";
