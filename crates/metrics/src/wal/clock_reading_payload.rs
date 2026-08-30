use super::*;

/// One clock confidence reading, plus the stamp the ingester wrote on it.
///
/// The host reports the reading. The ingester stamps [`Self::ingest_unix_nanos`]
/// from its own clock the moment the request arrives, and the difference
/// between the two is a measured skew between two named hosts. No single
/// exporter can compute that number, which is why the stamp lives here and not
/// on the wire.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClockReadingPayload {
    /// What the host reported.
    pub reading: DecodedClockReading,
    /// When this process received the reading, by its own clock.
    pub ingest_unix_nanos: UnixNanos,
}

impl ClockReadingPayload {
    /// The block timestamp for this reading, in epoch milliseconds.
    #[must_use]
    pub const fn timestamp_ms(&self) -> i64 {
        self.reading.timestamp_ms()
    }

    /// The skew between the host's clock and this ingester's clock.
    ///
    /// A positive extent means the host reads behind the ingester.
    #[must_use]
    pub fn ingest_skew(&self) -> Time {
        self.reading
            .reading_unix_nanos
            .extent_to(self.ingest_unix_nanos)
    }
}
