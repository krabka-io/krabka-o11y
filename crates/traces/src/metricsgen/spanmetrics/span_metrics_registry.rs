use super::*;

/// Pure fold that derives span-metrics RED series from spans.
#[derive(Debug)]
pub struct SpanMetricsRegistry {
    pub(crate) bucket_edges_ns: Vec<f64>,
    pub(crate) max_exemplars: usize,
    pub(crate) max_active_series: usize,
    pub(crate) enable_target_info: bool,
    pub(crate) enable_status_message: bool,
    pub(crate) config: SpanMetricsConfig,
    pub(crate) entries: HashMap<DimKey, DimEntry>,
    pub(crate) targets: HashSet<Vec<(String, String)>>,
    pub(crate) discarded_series: f64,
}

impl SpanMetricsRegistry {
    #[must_use]
    pub fn new(cfg: &MetricsGenConfig) -> Self {
        Self {
            bucket_edges_ns: cfg.histogram_buckets_ns.clone(),
            max_exemplars: cfg.max_exemplars_per_series,
            max_active_series: cfg.max_active_series,
            enable_target_info: cfg.enable_target_info,
            enable_status_message: cfg.enable_status_message,
            config: cfg.processor.span_metrics.clone(),
            entries: HashMap::new(),
            targets: HashSet::new(),
            discarded_series: 0.0,
        }
    }

    /// Fold one span into its RED series.
    ///
    /// This returns [`RecordOutcome::Dropped`] when the span needs a dimension
    /// key that the registry has no room for, and [`RecordOutcome::Recorded`]
    /// otherwise. Only those two of the four variants occur here. The variants
    /// [`RecordOutcome::Completed`] and [`RecordOutcome::Ignored`] belong to
    /// the edge store, which pairs two spans into one edge and skips a span of
    /// the wrong kind. Span metrics record every span they accept, and accept
    /// every kind, so a second enum would repeat this one's two live variants
    /// under a new name.
    pub fn record_span(&mut self, span: &SpanRecord) -> RecordOutcome {
        if !span_allowed(span, &self.config) {
            return RecordOutcome::Ignored;
        }
        let key = dim_key(span, self.enable_status_message, &self.config);
        let multiplier = span_multiplier(span, self.config.span_multiplier_key.as_deref());
        // Read the length before the entry borrow, because `entry` holds the
        // map for as long as the match below runs.
        let at_capacity =
            self.max_active_series != 0 && self.entries.len() >= self.max_active_series;
        let entry = match self.entries.entry(key) {
            Entry::Occupied(occupied) => occupied.into_mut(),
            Entry::Vacant(vacant) => {
                if at_capacity {
                    self.discarded_series += 1.0;
                    return RecordOutcome::Dropped;
                }
                // The service set is filled only after the key is admitted.
                // Every tracked service belongs to at least one entry, so the
                // entry cap bounds this set too and it needs no cap of its own.
                self.targets.insert(target_info_labels(span, &self.config));
                vacant.insert(DimEntry {
                    calls: 0.0,
                    size_total: 0.0,
                    latency: LatencyHistogram::new(&self.bucket_edges_ns),
                    exemplars: Vec::new(),
                })
            }
        };

        entry.calls += multiplier;
        entry.size_total += span.size.bytes_f64() * multiplier;
        let duration_ns = duration_as_f64(span.duration_ns);
        entry.latency.observe_weighted(duration_ns, multiplier);

        if entry.exemplars.len() < self.max_exemplars {
            entry.exemplars.push(Exemplar {
                value: duration_ns / NS_PER_SEC,
                labels: sorted_labels(vec![
                    ("trace_id".to_string(), hex::encode(span.trace_id)),
                    ("span_id".to_string(), hex::encode(span.span_id)),
                ]),
                timestamp_ms: span.start_ns / 1_000_000,
            });
        }
        RecordOutcome::Recorded
    }

    /// Emit the registry's RED series for this collection interval.
    ///
    /// The counters `calls_total` and `size_total`, and the latency histogram,
    /// are **cumulative**. The registry accumulates across intervals and emits
    /// the running total each time, which is Tempo's persistent-registry
    /// semantics. The `_total` series are therefore monotonic, and the
    /// consuming `PromQL` or Mimir `rate()` and `increase()` work.
    ///
    /// An earlier `drain` reset the accumulator every interval and emitted
    /// per-interval deltas under `_total` names. `PromQL` reads that as a
    /// counter reset on almost every scrape, which corrupts the headline RED
    /// rates.
    ///
    /// This drains only the per-sample **exemplars** each interval. They carry
    /// their own timestamp and must not be re-emitted. The accumulator resets
    /// only on a process restart, when the registry is rebuilt from WAL
    /// offsets, and `PromQL` treats that as a normal counter reset.
    #[must_use]
    pub fn drain(&mut self, timestamp_ms: i64) -> Vec<Series> {
        let mut series = Vec::with_capacity(self.entries.len() * 3 + self.targets.len() + 1);
        // Emitted on every interval, and cumulative like the RED counters
        // below it. The series therefore exists from the first interval, before
        // any refusal, and `PromQL` reads no counter reset. It carries no
        // labels: a counter labelled with the refused span's dimensions would
        // grow exactly the cardinality that the cap exists to hold down.
        series.push(Series {
            name: "traces_spanmetrics_discarded_series_total".to_string(),
            labels: Vec::new(),
            sample: SeriesSample::Counter(self.discarded_series),
            exemplars: Vec::new(),
            timestamp_ms,
        });

        for (labels, entry) in &mut self.entries {
            let labels = labels.clone();
            series.push(Series {
                name: "traces_spanmetrics_calls_total".to_string(),
                labels: labels.clone(),
                sample: SeriesSample::Counter(entry.calls),
                exemplars: Vec::new(),
                timestamp_ms,
            });
            series.push(Series {
                name: "traces_spanmetrics_size_total".to_string(),
                labels: labels.clone(),
                sample: SeriesSample::Counter(entry.size_total),
                exemplars: Vec::new(),
                timestamp_ms,
            });

            let (buckets, sum, count) = entry.latency.cumulative_seconds();
            series.push(Series {
                name: "traces_spanmetrics_latency".to_string(),
                labels,
                sample: SeriesSample::ClassicHistogram {
                    buckets,
                    sum,
                    count,
                },
                exemplars: std::mem::take(&mut entry.exemplars),
                timestamp_ms,
            });
        }

        if self.enable_target_info {
            series.extend(self.targets.iter().map(|labels| Series {
                name: "traces_target_info".to_string(),
                labels: labels.clone(),
                sample: SeriesSample::Gauge(1.0),
                exemplars: Vec::new(),
                timestamp_ms,
            }));
        }

        series
    }
}

fn target_info_labels(span: &SpanRecord, config: &SpanMetricsConfig) -> Vec<(String, String)> {
    let labels = std::iter::once(("service".to_string(), span.service_name.clone())).chain(
        span.resource_attributes
            .iter()
            .filter(|(name, _)| !config.target_info_excluded_dimensions.contains(name))
            .map(|(name, value)| (prometheus_label_name(name), value.clone())),
    );
    sorted_labels(labels.collect())
}
