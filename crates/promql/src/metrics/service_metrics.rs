use krabka_blockstore::ObjectStoreMetrics;
use krabka_metrics::metrics::{INGEST_HELP, METRICS_PREFIX, metrics_role_registry};
use krabka_observability::{
    RoleKind,
    service_metrics::{
        IngestInstruments, IngestRequest, QueryHelpText, QueryInstruments, QueryRequest,
        RequestOutcome, register_for_role, register_in_new_registry,
    },
    wal_consumer_metrics::WalConsumerMetrics,
};

use super::{
    ByteSize, Counter, Family, Gauge, Histogram, QueryTypeLabel, Registry, SharedRegistry, Time,
    TimeExt as _,
};

const QUERY_HELP: QueryHelpText = QueryHelpText {
    requests: "Query requests handled, labelled by route and outcome status.",
    duration: "Query handler latency in seconds, labelled by route.",
};

/// Bundle of metric handles that is cheap to clone. Build it one time with
/// [`ServiceMetrics::new`]. Give a clone, one `Arc::clone` each, to every
/// handler that emits a metric.
#[derive(Clone)]
pub struct ServiceMetrics {
    pub registry: SharedRegistry,
    pub object_store: ObjectStoreMetrics,
    pub wal_consumer: WalConsumerMetrics,
    // INGEST (distributor) role.
    /// Ingest requests, bytes, items, latency and WAL append failures.
    pub ingest: IngestInstruments,
    // QUERY (querier) role.
    /// Query requests and per-route handler latency.
    pub query: QueryInstruments,
    /// PromQL-engine evaluation latency (parse + plan + execute), labelled by
    /// query `type` (`instant`|`range`). This scope is narrower than
    /// `query.duration`, which covers the whole HTTP handler: param decode,
    /// permit wait, and encode.
    pub query_eval_duration: Family<QueryTypeLabel, Histogram>,
    /// Cumulative engine-eval failures, labelled by query `type`.
    pub query_errors: Family<QueryTypeLabel, Counter>,
    /// In-flight `PromQL` queries currently executing in the engine.
    pub active_queries: Gauge,
    /// Cumulative failed ruler rule evaluations.
    pub rule_evaluation_failures: Counter,
    /// Wall time of the most recently completed ruler group.
    pub rule_group_last_duration_seconds: Gauge<f64, std::sync::atomic::AtomicU64>,
    /// Whether this process currently owns its configured ruler shard.
    pub ruler_owner: Gauge,
    /// Producer id and epoch of the broker-enforced ruler fence.
    pub ruler_producer_id: Gauge,
    pub ruler_producer_epoch: Gauge,
    /// Cumulative lease advance and renewal failures.
    pub ruler_lease_renew_failures: Counter,
    /// Time between losing and reacquiring ruler ownership.
    pub ruler_failover_duration_seconds: Gauge<f64, std::sync::atomic::AtomicU64>,
}

impl ServiceMetrics {
    /// Builds a new registry, registers every metric, and returns the bundle.
    #[must_use]
    pub fn new() -> Self {
        register_in_new_registry(METRICS_PREFIX, Self::register)
    }

    /// Registers this role's instruments in the process registry.
    ///
    /// Each role has a separate prefix. Its gauges do not overwrite another role's gauges.
    pub async fn for_role(shared: SharedRegistry, role: RoleKind) -> Self {
        register_for_role(metrics_role_registry(shared, role), Self::register).await
    }

    fn register(registry: &mut Registry, shared: SharedRegistry) -> Self {
        let ingest = IngestInstruments::register(registry, &INGEST_HELP);
        let query = QueryInstruments::register(registry, &QUERY_HELP);

        let query_eval_duration: Family<QueryTypeLabel, Histogram> =
            Family::new_with_constructor(|| {
                Histogram::new([0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0])
            });
        let query_errors: Family<QueryTypeLabel, Counter> = Family::default();
        let active_queries = Gauge::default();
        let rule_evaluation_failures = Counter::default();
        let rule_group_last_duration_seconds = Gauge::default();
        let ruler_owner = Gauge::default();
        let ruler_producer_id = Gauge::default();
        let ruler_producer_epoch = Gauge::default();
        let ruler_lease_renew_failures = Counter::default();
        let ruler_failover_duration_seconds = Gauge::default();

        registry.register(
            "query_eval_duration_seconds",
            "PromQL engine evaluation latency in seconds (parse+plan+execute), labelled by query type.",
            query_eval_duration.clone(),
        );
        registry.register(
            "query_errors",
            "Cumulative PromQL engine evaluation failures, labelled by query type.",
            query_errors.clone(),
        );
        registry.register(
            "active_queries",
            "PromQL queries currently executing in the engine.",
            active_queries.clone(),
        );
        registry.register(
            "rule_evaluation_failures",
            "Cumulative failed ruler rule evaluations.",
            rule_evaluation_failures.clone(),
        );
        registry.register(
            "rule_group_last_duration_seconds",
            "Wall time in seconds of the most recently completed ruler group.",
            rule_group_last_duration_seconds.clone(),
        );
        registry.register(
            "ruler_owner",
            "Whether this process currently owns its configured ruler shard.",
            ruler_owner.clone(),
        );
        registry.register(
            "ruler_producer_id",
            "Producer id of the broker-enforced ruler fencing token.",
            ruler_producer_id.clone(),
        );
        registry.register(
            "ruler_producer_epoch",
            "Producer epoch of the broker-enforced ruler fencing token.",
            ruler_producer_epoch.clone(),
        );
        registry.register(
            "ruler_lease_renew_failures",
            "Cumulative ruler lease advance and renewal failures.",
            ruler_lease_renew_failures.clone(),
        );
        registry.register(
            "ruler_failover_duration_seconds",
            "Time between losing and reacquiring ruler ownership.",
            ruler_failover_duration_seconds.clone(),
        );
        let object_store = ObjectStoreMetrics::register(registry);
        let wal_consumer = WalConsumerMetrics::register(registry);

        Self {
            registry: shared,
            object_store,
            wal_consumer,
            ingest,
            query,
            query_eval_duration,
            query_errors,
            active_queries,
            rule_evaluation_failures,
            rule_group_last_duration_seconds,
            ruler_owner,
            ruler_producer_id,
            ruler_producer_epoch,
            ruler_lease_renew_failures,
            ruler_failover_duration_seconds,
        }
    }

    /// Records one ingest request outcome.
    ///
    /// This method does NOT touch `wal_append_failures`. Increment that counter
    /// at the WAL or produce error site, so that a 4xx client or validation
    /// error does not inflate the WAL-failure counter.
    pub fn record_ingest(&self, ok: bool, size: ByteSize, items: u64, latency: Time) {
        self.ingest.record(IngestRequest {
            outcome: if ok {
                RequestOutcome::Ok
            } else {
                RequestOutcome::Error
            },
            body: size,
            items,
            elapsed: latency,
        });
    }

    /// Records one query request outcome on `route` with its latency.
    pub fn record_query(&self, route: &str, ok: bool, latency: Time) {
        self.query.record(QueryRequest {
            route,
            outcome: if ok {
                RequestOutcome::Ok
            } else {
                RequestOutcome::Error
            },
            // Prometheus histograms are in base units, so the latency lands in
            // seconds no matter what unit the caller measured it in.
            elapsed_secs: latency.secs_f64(),
        });
    }

    /// Records one `PromQL` engine evaluation.
    ///
    /// This method observes `latency` under `query_eval_duration{type}`. When
    /// `ok` is false, it also increments `query_errors{type}`. `query_type` is
    /// `"instant"` or `"range"`.
    pub fn record_eval(&self, query_type: &str, ok: bool, latency: Time) {
        self.query_eval_duration
            .get_or_create(&QueryTypeLabel {
                r#type: query_type.into(),
            })
            .observe(latency.secs_f64());
        if !ok {
            self.query_errors
                .get_or_create(&QueryTypeLabel {
                    r#type: query_type.into(),
                })
                .inc();
        }
    }

    /// Increments `active_queries` at query entry, without an RAII guard.
    ///
    /// Pair every call with [`Self::query_finished`].
    pub fn query_started(&self) {
        self.active_queries.inc();
    }

    /// Decrements `active_queries` at query exit. Pairs with
    /// [`Self::query_started`].
    pub fn query_finished(&self) {
        self.active_queries.dec();
    }

    /// Records one rule outcome and its containing group's duration.
    pub fn record_ruler_rule(&self, ok: bool) {
        if !ok {
            self.rule_evaluation_failures.inc();
        }
    }

    pub fn record_ruler_group(&self, duration_seconds: f64) {
        self.rule_group_last_duration_seconds
            .set(duration_seconds.max(0.0));
    }
}

impl Default for ServiceMetrics {
    fn default() -> Self {
        Self::new()
    }
}
