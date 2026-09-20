use super::Duration;

/// Maximum time from starting a merge attempt to publishing its manifest.
///
/// This is deliberately much shorter than [`SHARD_PAYLOAD_SWEEP_GRACE`]. If
/// the timeout wins, the manifest put is cancelled, so a payload cannot age
/// into sweep eligibility and then be published by the same attempt.
pub(crate) const SHARD_PAYLOAD_PUBLISH_TIMEOUT: Duration = Duration::from_mins(5);

/// How long an unreferenced shard payload is left alone before the sweep may
/// reclaim it.
///
/// A writer puts its payloads and only then swaps the manifest that names
/// them, so between those two steps its payloads are referenced by nothing and
/// look exactly like orphans. Reclaiming one there would leave the winner's
/// manifest naming an object that is gone. Nothing in the protocol tells the
/// sweeper which is which, so the sweeper waits: an hour is far longer than
/// the enforced timeout on one publication attempt, and costs only that an
/// orphan survives an hour longer than it had to.
pub(crate) const SHARD_PAYLOAD_SWEEP_GRACE: Duration = Duration::from_hours(1);
