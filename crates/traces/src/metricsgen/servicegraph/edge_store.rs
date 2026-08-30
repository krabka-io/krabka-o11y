use super::*;

/// Bounded, TTL'd service-graph edge store.
#[derive(Debug)]
pub struct EdgeStore {
    pub(crate) max_items: usize,
    pub(crate) ttl: Time,
    pub(crate) enable_messaging_latency: bool,
    pub(crate) bucket_edges_ns: Vec<f64>,
    pub(crate) edges: HashMap<EdgeKey, Edge>,
    pub(crate) aggregates: HashMap<LabelKey, EdgeAgg>,
    pub(crate) unpaired: HashMap<LabelKey, f64>,
    pub(crate) dropped: HashMap<LabelKey, f64>,
}

impl EdgeStore {
    #[must_use]
    pub fn new(cfg: &MetricsGenConfig) -> Self {
        Self {
            max_items: cfg.edge_store_max_items,
            ttl: cfg.edge_ttl,
            enable_messaging_latency: cfg.enable_messaging_system_latency,
            bucket_edges_ns: cfg.histogram_buckets_ns.clone(),
            edges: HashMap::new(),
            aggregates: HashMap::new(),
            unpaired: HashMap::new(),
            dropped: HashMap::new(),
        }
    }

    ///
    /// # Panics
    /// Panics if an internal synchronization primitive is poisoned.
    pub fn record_span(&mut self, span: &SpanRecord, now_ns: i64) -> RecordOutcome {
        let Some(is_client) = edge_side(span.kind) else {
            return RecordOutcome::Ignored;
        };

        let Some(key) = edge_key(span) else {
            return RecordOutcome::Ignored;
        };
        self.expire(now_ns);

        let connection_type = classify(span);
        let failed = span.status == StatusCode::Error;
        let latency_ns = span.duration_ns.max(0);

        if let Some(edge) = self.edges.get_mut(&key) {
            fill_edge(edge, span, is_client, latency_ns);
            edge.failed |= failed;
            if connection_type != ConnectionType::Unset {
                edge.connection_type = connection_type;
            }
            // Backfill the peer.service virtual node on the update path too, so the
            // result is order-independent: an edge that transitions to (or already
            // is) VirtualNode gets its peer label set regardless of which span
            // carried the signal first.
            let edge_connection_type = edge.connection_type;
            fill_virtual_node(edge, span, is_client, edge_connection_type);
            if edge.client_service.is_some() && edge.server_service.is_some() {
                let edge = self.edges.remove(&key).expect("edge exists after get_mut");
                self.complete(edge);
                return RecordOutcome::Completed;
            }
            return RecordOutcome::Recorded;
        }

        if self.edges.len() >= self.max_items {
            *self
                .dropped
                .entry(label_key_for_span(span, is_client, connection_type))
                .or_insert(0.0) += 1.0;
            return RecordOutcome::Dropped;
        }

        let mut edge = Edge {
            client_service: None,
            server_service: None,
            client_latency_ns: None,
            server_latency_ns: None,
            failed,
            connection_type,
            first_seen_ns: now_ns,
        };
        fill_edge(&mut edge, span, is_client, latency_ns);
        fill_virtual_node(&mut edge, span, is_client, connection_type);
        self.edges.insert(key, edge);
        RecordOutcome::Recorded
    }

    ///
    /// # Panics
    /// Panics if an internal synchronization primitive is poisoned.
    pub fn expire(&mut self, now_ns: i64) -> usize {
        let expired: Vec<_> = self
            .edges
            .iter()
            .filter(|(_, edge)| {
                Time::from_nanos(now_ns.saturating_sub(edge.first_seen_ns)) >= self.ttl
            })
            .map(|(key, _)| *key)
            .collect();

        for key in &expired {
            let edge = self.edges.remove(key).expect("expired key exists");
            *self
                .unpaired
                .entry(label_key_for_edge(&edge))
                .or_insert(0.0) += 1.0;
        }

        expired.len()
    }

    #[must_use]
    pub fn checkpoint_entries(&self, tenant: &str) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut entries: Vec<_> = self
            .edges
            .iter()
            .map(|((trace_id, edge_id), edge)| {
                (
                    encode_checkpoint_key(tenant, trace_id, edge_id).to_vec(),
                    encode_checkpoint_value(edge),
                )
            })
            .collect();
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        entries
    }

    ///
    /// # Errors
    /// Returns an error when the query is malformed, an expression has incompatible operand types, or the backing span store fails.
    pub fn restore_checkpoint_entry(
        &mut self,
        tenant: &str,
        key: &[u8],
        value: &[u8],
    ) -> Result<(), CheckpointCodecError> {
        let (encoded_tenant, trace_id, edge_id) = parse_checkpoint_key(key)?;
        if encoded_tenant != tenant {
            return Ok(());
        }
        let edge_id: [u8; 8] = edge_id
            .try_into()
            .map_err(|_| CheckpointCodecError::BadEdgeId)?;
        let edge = decode_checkpoint_value(value)?;
        self.edges.insert((trace_id, edge_id), edge);
        Ok(())
    }

    #[must_use]
    pub fn drain(&mut self, timestamp_ms: i64) -> Vec<Series> {
        let mut out = Vec::new();

        for ((client, server, connection_type), agg) in self.aggregates.drain() {
            let labels = sorted_labels(vec![
                ("client".to_string(), client),
                ("server".to_string(), server),
                (
                    "connection_type".to_string(),
                    connection_type.as_label().to_string(),
                ),
            ]);
            out.push(counter(
                "traces_service_graph_request_total",
                &labels,
                agg.requests,
                timestamp_ms,
            ));
            out.push(counter(
                "traces_service_graph_request_failed_total",
                &labels,
                agg.failed,
                timestamp_ms,
            ));
            push_histogram(
                &mut out,
                "traces_service_graph_request_client_seconds",
                &labels,
                HistogramSnapshot {
                    sum: agg.client_seconds_sum,
                    count: agg.client_seconds_count,
                    bucket_edges_ns: &self.bucket_edges_ns,
                    bucket_counts: &agg.client_bucket_counts,
                },
                timestamp_ms,
            );
            push_histogram(
                &mut out,
                "traces_service_graph_request_server_seconds",
                &labels,
                HistogramSnapshot {
                    sum: agg.server_seconds_sum,
                    count: agg.server_seconds_count,
                    bucket_edges_ns: &self.bucket_edges_ns,
                    bucket_counts: &agg.server_bucket_counts,
                },
                timestamp_ms,
            );
            if self.enable_messaging_latency {
                push_histogram(
                    &mut out,
                    "traces_service_graph_request_messaging_system_seconds",
                    &labels,
                    HistogramSnapshot {
                        sum: agg.messaging_seconds_sum,
                        count: agg.messaging_seconds_count,
                        bucket_edges_ns: &self.bucket_edges_ns,
                        bucket_counts: &agg.messaging_bucket_counts,
                    },
                    timestamp_ms,
                );
            }
        }

        for (label_key, value) in self.unpaired.drain() {
            let labels = service_graph_labels(label_key);
            out.push(counter(
                "traces_service_graph_unpaired_spans_total",
                &labels,
                value,
                timestamp_ms,
            ));
        }

        for (label_key, value) in self.dropped.drain() {
            let labels = service_graph_labels(label_key);
            out.push(counter(
                "traces_service_graph_dropped_spans_total",
                &labels,
                value,
                timestamp_ms,
            ));
        }

        out
    }

    pub(crate) fn complete(&mut self, edge: Edge) {
        let client = edge.client_service.unwrap_or_default();
        let server = edge.server_service.unwrap_or_default();
        let bucket_count = self.bucket_edges_ns.len() + 1;
        let agg = self
            .aggregates
            .entry((client, server, edge.connection_type))
            .or_insert_with(|| EdgeAgg::new(bucket_count));
        agg.requests += 1.0;
        if edge.failed {
            agg.failed += 1.0;
        }
        if let Some(ns) = edge.client_latency_ns {
            agg.client_seconds_sum += ns_to_seconds(ns);
            agg.client_seconds_count += 1.0;
            observe_latency(&self.bucket_edges_ns, &mut agg.client_bucket_counts, ns);
        }
        if let Some(ns) = edge.server_latency_ns {
            agg.server_seconds_sum += ns_to_seconds(ns);
            agg.server_seconds_count += 1.0;
            observe_latency(&self.bucket_edges_ns, &mut agg.server_bucket_counts, ns);
        }
        if self.enable_messaging_latency
            && edge.connection_type == ConnectionType::MessagingSystem
            && let Some(ns) = edge.server_latency_ns.or(edge.client_latency_ns)
        {
            agg.messaging_seconds_sum += ns_to_seconds(ns);
            agg.messaging_seconds_count += 1.0;
            observe_latency(&self.bucket_edges_ns, &mut agg.messaging_bucket_counts, ns);
        }
    }
}
