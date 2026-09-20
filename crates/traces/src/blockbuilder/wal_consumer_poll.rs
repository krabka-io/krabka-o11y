use super::*;

/// Minimal WAL-consumer poll surface the block-builder loop drives.
///
/// `run` takes this trait rather than the concrete
/// [`krabka_client_consumer::Consumer`], so a scripted fake can drive the
/// offset-commit invariants in tests. The record type matches what
/// [`decode_consumer_records`] consumes, so the loop body stays the same.
#[async_trait::async_trait]
pub trait WalConsumerPoll: Send {
    fn take_revoked_partitions(&mut self) -> BTreeSet<i32> {
        BTreeSet::new()
    }

    async fn poll(&mut self, window: Time) -> Result<Vec<ConsumerRecord>, TracesError>;
}
