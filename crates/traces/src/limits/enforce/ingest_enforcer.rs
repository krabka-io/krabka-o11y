use super::*;

#[derive(Debug, Default)]
pub struct IngestEnforcer {
    pub(crate) buckets: DashMap<String, Arc<RateBucket>>,
}

impl IngestEnforcer {
    #[must_use]
    pub fn new() -> Self {
        Self {
            buckets: DashMap::new(),
        }
    }

    ///
    /// # Errors
    /// Returns an error when the query is malformed, an expression has incompatible operand types, or the backing span store fails.
    pub fn check_span_rate(
        &self,
        limits: &Limits,
        tenant: &str,
        n_spans: u64,
    ) -> Result<(), LimitError> {
        if limits.ingestion_rate.per_sec_f64() == 0.0 || n_spans == 0 {
            return Ok(());
        }
        let rate = rounded_positive_rate(limits.ingestion_rate.per_sec_f64());
        if rate == 0 {
            return Ok(());
        }

        // Refill rate and burst capacity are seeded separately: the sustained
        // refill tracks the configured spans/sec, while the bucket capacity is
        // the larger of rate and configured burst so a burst can be absorbed
        // without raising the sustained rate.
        //
        // NOTE: `krabka_broker::throttle::TokenBucket` couples refill rate and
        // capacity in a single `set_rate` (capacity == rate, no separate burst
        // knob) and offers no peek/refund, so it cannot express either a
        // distinct burst capacity (M4) or all-or-nothing consumption. We use a
        // local `RateBucket` (same refill math) that decouples the two and
        // consumes atomically all-or-nothing instead of editing the broker crate.
        let capacity = rate.max(limits.ingestion_burst_spans);
        let bucket = self
            .buckets
            .entry(tenant.to_string())
            .or_insert_with(|| Arc::new(RateBucket::new(rate, capacity)))
            .clone();
        // All-or-nothing: a rejected over-limit request consumes no tokens, so
        // it does not starve a subsequent within-limit request.
        if bucket.try_consume_all(n_spans) {
            Ok(())
        } else {
            Err(LimitError::IngestionRateExceeded {
                rate: limits.ingestion_rate.per_sec_f64(),
                observed: f64_from_u64(n_spans),
            })
        }
    }

    ///
    /// # Errors
    /// Returns an error when the query is malformed, an expression has incompatible operand types, or the backing span store fails.
    pub fn check_trace_size(limits: &Limits, spans_in_trace: u64) -> Result<(), LimitError> {
        let limit = limits.max_spans_per_trace;
        if limit != 0 && spans_in_trace > limit {
            return Err(LimitError::MaxSpansPerTrace {
                limit,
                observed: spans_in_trace,
            });
        }
        Ok(())
    }

    /// Enforce the per-attribute byte cap.
    ///
    /// Each entry is `(key, value_bytes)`, where `value_bytes` is the value's
    /// TRUE encoded byte length. Callers must therefore measure `Bytes`, `Int`
    /// and `Double` values, and must not convert them to a string first.
    ///
    /// # Errors
    /// Returns an error when the query is malformed, an expression has incompatible operand types, or the backing span store fails.
    pub fn check_attributes(limits: &Limits, attrs: &[(String, u64)]) -> Result<(), LimitError> {
        let limit = limits.max_attribute.bytes_u64();
        if limit == 0 {
            return Ok(());
        }
        for (key, value_bytes) in attrs {
            let observed = (key.len() as u64).max(*value_bytes);
            if observed > limit {
                return Err(LimitError::AttributeTooLong { limit, observed });
            }
        }
        Ok(())
    }
}
