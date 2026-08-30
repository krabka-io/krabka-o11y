use super::*;

/// Pure fold that derives span-metrics RED series from spans.
#[derive(Debug)]
pub struct SpanMetricsRegistry {
    pub(crate) bucket_edges_ns: Vec<f64>,
    pub(crate) max_exemplars: usize,
    pub(crate) enable_target_info: bool,
    pub(crate) enable_status_message: bool,
    pub(crate) entries: HashMap<DimKey, DimEntry>,
    pub(crate) services: HashSet<String>,
}

impl SpanMetricsRegistry {
    #[must_use]
    pub fn new(cfg: &MetricsGenConfig) -> Self {
        Self {
            bucket_edges_ns: cfg.histogram_buckets_ns.clone(),
            max_exemplars: cfg.max_exemplars_per_series,
            enable_target_info: cfg.enable_target_info,
            enable_status_message: cfg.enable_status_message,
            entries: HashMap::new(),
            services: HashSet::new(),
        }
    }

    pub fn record_span(&mut self, span: &SpanRecord) {
        let key = dim_key(span, self.enable_status_message);
        self.services.insert(span.service_name.clone());
        let bucket_edges_ns = self.bucket_edges_ns.clone();
        let entry = self.entries.entry(key).or_insert_with(|| DimEntry {
            calls: 0.0,
            size_total: 0.0,
            latency: LatencyHistogram::new(&bucket_edges_ns),
            exemplars: Vec::new(),
        });

        entry.calls += 1.0;
        entry.size_total += span.size.bytes_f64();
        let duration_ns = duration_as_f64(span.duration_ns);
        entry.latency.observe(duration_ns);

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
        let mut series = Vec::with_capacity(self.entries.len() * 3 + self.services.len());

        for ((service, span_name, span_kind, status_code, status_message), entry) in
            &mut self.entries
        {
            let mut labels = vec![
                ("service".to_string(), service.clone()),
                ("span_name".to_string(), span_name.clone()),
                ("span_kind".to_string(), span_kind.clone()),
                ("status_code".to_string(), status_code.clone()),
            ];
            if let Some(status_message) = status_message {
                labels.push(("status_message".to_string(), status_message.clone()));
            }
            let labels = sorted_labels(labels);
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
            series.extend(self.services.iter().map(|service| Series {
                name: "traces_target_info".to_string(),
                labels: sorted_labels(vec![("service".to_string(), service.clone())]),
                sample: SeriesSample::Gauge(1.0),
                exemplars: Vec::new(),
                timestamp_ms,
            }));
        }

        series
    }
}
