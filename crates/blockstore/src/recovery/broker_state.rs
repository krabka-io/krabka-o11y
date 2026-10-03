use super::{BrokerSnapshot, async_trait};

/// Reads the broker state that a deployment cut records.
///
/// `krabka-blockstore` does not speak the Kafka protocol. A caller supplies
/// the broker side, and a test supplies a fake.
#[async_trait]
pub trait BrokerState: Send + Sync {
    /// Reads the next offset of every partition in the cut and every
    /// committed group offset on those partitions.
    ///
    /// # Errors
    ///
    /// Returns a description when the broker cannot be read.
    async fn capture(&self) -> Result<BrokerSnapshot, String>;

    /// Counts the records that a `read_committed` consumer reads from
    /// `offset` up to `next_offset`, the end that [`Self::capture`] reported.
    ///
    /// A transaction marker and an aborted record take an offset and are not
    /// records. A block builder never commits past a trailing marker, so a
    /// drained group can lag the next offset by markers only. The default
    /// counts every offset as a record, which is exact for a topic that no
    /// transactional producer writes.
    ///
    /// # Errors
    ///
    /// Returns a description when the partition cannot be read.
    async fn records_between(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
        next_offset: i64,
    ) -> Result<u64, String> {
        let _ = (topic, partition);
        Ok(u64::try_from(next_offset.saturating_sub(offset)).unwrap_or(0))
    }
}
