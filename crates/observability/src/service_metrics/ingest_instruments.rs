use super::{
    ByteSize, ByteSizeExt as _, Counter, Family, Histogram, Registry, RequestOutcome, StatusLabel,
    Time, TimeExt as _,
};

/// Latency buckets, in seconds, of the `ingest_duration_seconds` histogram.
const INGEST_DURATION_BUCKETS: [f64; 11] = [
    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0,
];

/// The ingest (distributor) instruments every signal exports: the request,
/// byte, item and latency families and the WAL append-failure counter.
#[derive(Clone)]
pub struct IngestInstruments {
    /// Ingest requests, labelled by outcome: `ingest_requests_total{status}`.
    pub requests: Family<StatusLabel, Counter>,
    /// Cumulative request-body bytes accepted: `ingest_bytes_total`.
    pub bytes: Counter,
    /// Cumulative items accepted: `ingest_items_total`.
    pub items: Counter,
    /// Ingest handler latency: `ingest_duration_seconds`.
    pub duration: Histogram,
    /// Cumulative WAL/produce append failures: `wal_append_failures_total`.
    /// [`IngestInstruments::record`] does not touch it. The WAL or produce
    /// error site bumps it, so a 4xx client or validation error does not
    /// inflate it.
    pub wal_append_failures: Counter,
}

/// The `# HELP` text of each ingest instrument. Each signal words its own.
#[derive(Debug, Clone, Copy)]
pub struct IngestHelpText {
    pub requests: &'static str,
    pub bytes: &'static str,
    pub items: &'static str,
    pub duration: &'static str,
    pub wal_append_failures: &'static str,
}

/// One ingest request outcome, as [`IngestInstruments::record`] takes it.
#[derive(Debug, Clone, Copy)]
pub struct IngestRequest {
    pub outcome: RequestOutcome,
    /// Request-body size.
    pub body: ByteSize,
    /// Accepted items. It is dimensionless, so it stays an integer.
    pub items: u64,
    /// Handler latency.
    pub elapsed: Time,
}

impl IngestInstruments {
    /// Registers the ingest instruments in `registry`, in the order
    /// `ingest_requests`, `ingest_bytes`, `ingest_items`,
    /// `ingest_duration_seconds`, `wal_append_failures`.
    pub fn register(registry: &mut Registry, help: &IngestHelpText) -> Self {
        let instruments = Self {
            requests: Family::default(),
            bytes: Counter::default(),
            items: Counter::default(),
            duration: Histogram::new(INGEST_DURATION_BUCKETS),
            wal_append_failures: Counter::default(),
        };
        registry.register(
            "ingest_requests",
            help.requests,
            instruments.requests.clone(),
        );
        registry.register("ingest_bytes", help.bytes, instruments.bytes.clone());
        registry.register("ingest_items", help.items, instruments.items.clone());
        registry.register(
            "ingest_duration_seconds",
            help.duration,
            instruments.duration.clone(),
        );
        registry.register(
            "wal_append_failures",
            help.wal_append_failures,
            instruments.wal_append_failures.clone(),
        );
        instruments
    }

    /// Records one ingest request outcome. It bumps the per-status request
    /// counter, accumulates bytes and items, and observes the handler latency.
    ///
    /// It converts the body size and the latency to the raw bytes and seconds
    /// the Prometheus instruments hold, so a caller never spells out `_bytes`
    /// or `_secs`.
    pub fn record(&self, request: IngestRequest) {
        self.requests
            .get_or_create(&StatusLabel {
                status: request.outcome.status().into(),
            })
            .inc();
        self.bytes.inc_by(request.body.bytes_u64());
        self.items.inc_by(request.items);
        self.duration.observe(request.elapsed.secs_f64());
    }

    /// Bumps the WAL/produce append-failure counter. Call it only for an
    /// actual WAL (Kafka produce) error, not for a client or validation 4xx.
    pub fn record_wal_append_failure(&self) {
        self.wal_append_failures.inc();
    }
}
