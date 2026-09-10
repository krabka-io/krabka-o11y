use super::Duration;

/// How long an unreferenced shard payload is left alone before the sweep may
/// delete it.
///
/// A writer puts its payloads and only then swaps the manifest that names
/// them, so between those two steps its payloads are referenced by nothing and
/// look exactly like orphans. Deleting one there would leave the winner's
/// manifest naming an object that is gone. Nothing in the protocol tells the
/// sweeper which is which, so the sweeper waits: an hour is far longer than
/// the bounded retry loop in [`super::put_manifest_snapshot`] can take, and
/// costs only that an orphan survives an hour longer than it had to.
pub(crate) const SHARD_PAYLOAD_SWEEP_GRACE: Duration = Duration::from_hours(1);
