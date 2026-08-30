use super::*;

/// Per-tenant byte token bucket for the `producer_byte_rate` ingest quota.
///
/// `updated_at` stays an [`Instant`] because it is a coordinate and not an
/// extent. The extent measured from it multiplies the rate into a byte
/// allowance.
#[derive(Debug)]
pub(crate) struct IngestQuotaBucket {
    pub(crate) rate: ByteRate,
    pub(crate) burst_window: Time,
    pub(crate) available: ByteSize,
    pub(crate) updated_at: Instant,
}

impl IngestQuotaBucket {
    pub(crate) fn new(rate: ByteRate, burst_window: Time) -> Self {
        Self {
            rate,
            burst_window,
            available: Self::burst_capacity(rate, burst_window),
            updated_at: Instant::now(),
        }
    }

    pub(crate) fn update_rate(&mut self, rate: ByteRate) {
        self.refill();
        self.rate = rate;
        // `>` is a permanent mutation survivor against `>=`: the two differ
        // only when the two are already equal, and then the assignment stores
        // the value already held.
        if self.available > self.capacity() {
            self.available = self.capacity();
        }
    }

    pub(crate) fn consume(&mut self, size: ByteSize) -> bool {
        self.refill();
        if size > self.available {
            return false;
        }
        self.available -= size;
        true
    }

    pub(crate) fn refill(&mut self) {
        let now = Instant::now();
        let elapsed = now.duration_since(self.updated_at).as_time();
        self.updated_at = now;
        // `ByteRate * Time` is a `ByteSize`, checked by the compiler.
        let refilled: ByteSize = (self.rate * elapsed).into();
        self.available = (self.available + refilled).min(self.capacity());
    }

    pub(crate) fn capacity(&self) -> ByteSize {
        Self::burst_capacity(self.rate, self.burst_window)
    }

    pub(crate) fn burst_capacity(rate: ByteRate, burst_window: Time) -> ByteSize {
        (rate * burst_window).into()
    }
}
