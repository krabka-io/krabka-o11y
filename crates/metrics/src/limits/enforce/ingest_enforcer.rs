use super::*;

#[derive(Debug)]
pub struct IngestEnforcer {
    pub(crate) sample_rate_buckets: DashMap<String, RateBucket>,
    /// Maximum number of distinct tenants tracked in `sample_rate_buckets`.
    pub(crate) max_rate_buckets: usize,
    /// Monotonic logical clock that stamps bucket touches for LRU eviction.
    pub(crate) touch_clock: AtomicU64,
}

impl Default for IngestEnforcer {
    fn default() -> Self {
        Self {
            sample_rate_buckets: DashMap::new(),
            max_rate_buckets: DEFAULT_MAX_RATE_BUCKETS,
            touch_clock: AtomicU64::new(0),
        }
    }
}

impl IngestEnforcer {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Constructs an enforcer that tracks at most `max_rate_buckets` distinct
    /// tenants for ingestion-rate limiting. A value of `0` clamps to `1`.
    #[must_use]
    pub fn with_max_rate_buckets(max_rate_buckets: usize) -> Self {
        Self {
            max_rate_buckets: max_rate_buckets.max(1),
            ..Self::default()
        }
    }

    #[cfg(test)]
    pub(crate) const fn max_rate_buckets(&self) -> usize {
        self.max_rate_buckets
    }

    /// Next value of the monotonic logical clock used to stamp bucket touches.
    pub(crate) fn next_touch_stamp(&self) -> u64 {
        self.touch_clock.fetch_add(1, Ordering::Relaxed)
    }

    /// Evicts the least-recently-touched tenants until the map is within the
    /// cap.
    ///
    /// This runs only on the cold path, where a new tenant arrives while the
    /// map is already at capacity. `max_rate_buckets` bounds the linear scan,
    /// and the scan never runs on the steady-state hot path.
    pub(crate) fn evict_if_over_cap(&self) {
        while self.sample_rate_buckets.len() > self.max_rate_buckets {
            let oldest = self
                .sample_rate_buckets
                .iter()
                .min_by_key(|entry| entry.value().last_touch.load(Ordering::Relaxed))
                .map(|entry| entry.key().clone());
            match oldest {
                Some(key) => {
                    self.sample_rate_buckets.remove(&key);
                }
                None => break,
            }
        }
    }

    /// # Errors
    /// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
    pub fn check_sample_rate(
        &self,
        limits: &Limits,
        tenant: &str,
        n_samples: u64,
    ) -> Result<(), LimitError> {
        // Only a finite, strictly-positive rate of zero disables the limit
        // (Mimir's `ingestion_rate: 0` sentinel). A non-finite rate (NaN/Inf)
        // never reaches the unlimited path: NaN would slip past `== 0`, and
        // Inf is treated as effectively unbounded throughput, both of which we
        // collapse to "rate limiting disabled" rather than the integer-bucket
        // path, which cannot represent them.
        let configured = limits.ingestion_rate.per_sec_f64();
        if !configured.is_finite() || configured <= 0.0 {
            return Ok(());
        }

        // A configured positive rate must never round down to `0`, which the
        // token bucket interprets as the unlimited sentinel. Round to nearest
        // but clamp to at least one sample/sec so e.g. `0.4` still throttles.
        let rate = configured.round().to_i64().unwrap_or(i64::MAX).max(1);
        let stamp = self.next_touch_stamp();
        let entry = self
            .sample_rate_buckets
            .entry(tenant.to_string())
            .or_insert_with(|| {
                let bucket = Arc::new(TokenBucket::new());
                // One token is one sample here, so this goes through the
                // bucket's event-rate pair rather than its byte-rate pair.
                bucket.set_event_rate_with_burst(
                    Frequency::from_per_sec_u64(u64::try_from(rate).unwrap_or(u64::MAX)),
                    limits.ingestion_burst_size,
                );
                RateBucket {
                    bucket,
                    last_touch: AtomicU64::new(stamp),
                }
            });
        // Stamp this access for LRU eviction, then drop the dashmap entry guard
        // before scanning so the eviction sweep never contends with the shard
        // lock this tenant lives in.
        entry.last_touch.store(stamp, Ordering::Relaxed);
        let bucket = entry.bucket.clone();
        drop(entry);
        self.evict_if_over_cap();
        let granted = bucket.try_consume(n_samples);
        if granted == n_samples {
            Ok(())
        } else {
            Err(LimitError::IngestionRateExceeded {
                rate: configured,
                observed: n_samples.to_f64().unwrap_or(f64::MAX),
            })
        }
    }

    /// # Errors
    /// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
    pub fn check_active_series(
        &self,
        limits: &Limits,
        _tenant: &str,
        would_add: u64,
        current: u64,
    ) -> Result<(), LimitError> {
        if limits.max_global_series_per_user == 0 {
            return Ok(());
        }
        let observed = current.saturating_add(would_add);
        if observed > limits.max_global_series_per_user {
            Err(LimitError::MaxSeriesPerUser {
                limit: limits.max_global_series_per_user,
                observed,
            })
        } else {
            Ok(())
        }
    }

    /// # Errors
    /// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
    pub fn check_labels(limits: &Limits, labels: &Labels) -> Result<(), LimitError> {
        for (name, value) in labels.iter() {
            let name_len = ByteSize::from_bytes(u64::try_from(name.len()).unwrap_or(u64::MAX));
            if name_len > limits.max_label_name_length {
                return Err(LimitError::LabelNameTooLong {
                    limit: limits.max_label_name_length.bytes_u64(),
                    observed: name_len.bytes_u64(),
                });
            }
            let value_len = ByteSize::from_bytes(u64::try_from(value.len()).unwrap_or(u64::MAX));
            if value_len > limits.max_label_value_length {
                return Err(LimitError::LabelValueTooLong {
                    limit: limits.max_label_value_length.bytes_u64(),
                    observed: value_len.bytes_u64(),
                });
            }
        }
        Ok(())
    }
}
