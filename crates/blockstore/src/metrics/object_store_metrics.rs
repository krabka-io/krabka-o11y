use super::{
    ByteSize, ByteSizeExt, Counter, Family, Histogram, ObjectStoreOperation,
    ObjectStoreOperationLabel, Registry, Time, TimeExt,
};

/// Latency buckets for one object-store request, in seconds.
///
/// The range runs from a local-filesystem write to a request that is about to
/// time out. It matches the spread
/// `thanos_objstore_bucket_operation_duration_seconds` uses, so a panel that
/// reads a Loki or Mimir bucket histogram reads this one with the same
/// quantile expression.
const OPERATION_DURATION_BUCKETS: [f64; 12] = [
    0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 15.0, 60.0,
];

/// The object-store instruments, as a cheaply-clonable bundle of handles.
///
/// Build one with [`ObjectStoreMetrics::register`] and give it to
/// [`MeteredObjectStore::wrap`](super::MeteredObjectStore::wrap). Every clone
/// is a handful of `Arc::clone`s and every handle is an atomic, so a recording
/// call takes no lock and never waits for the exporter.
#[derive(Clone, Debug)]
pub struct ObjectStoreMetrics {
    operations: Family<ObjectStoreOperationLabel, Counter>,
    failures: Family<ObjectStoreOperationLabel, Counter>,
    retries: Family<ObjectStoreOperationLabel, Counter>,
    transferred_bytes: Family<ObjectStoreOperationLabel, Counter>,
    duration: Family<ObjectStoreOperationLabel, Histogram>,
}

impl ObjectStoreMetrics {
    /// Registers the instruments into an `objstore` sub-registry of
    /// `registry`, and returns the handles.
    ///
    /// A service calls this once, from the constructor of its own
    /// `ServiceMetrics`, so the names inherit the service prefix: the traces
    /// service exports `krabka_traces_objstore_operations_total`.
    pub fn register(registry: &mut Registry) -> Self {
        let this = Self::unregistered();
        let registry = registry.sub_registry_with_prefix("objstore");

        registry.register(
            "operations",
            "Object-store requests attempted, by operation. A retried request \
             counts once per attempt, because that is what the store saw.",
            this.operations.clone(),
        );
        registry.register(
            "operation_failures",
            "Object-store request attempts that failed, by operation. A \
             transient failure that the next attempt recovered from is \
             counted here as well.",
            this.failures.clone(),
        );
        registry.register(
            "operation_retries",
            "Object-store requests retried after a transient failure, by \
             operation. A store that degrades but still succeeds on a later \
             attempt moves only this counter. The `write_block` operation \
             appears here and nowhere else: a whole block write is what the \
             BlockWriter retries, and underneath it the puts are counted \
             under their own label.",
            this.retries.clone(),
        );
        registry.register(
            "operation_transferred_bytes",
            "Payload bytes an object-store request carried, by operation.",
            this.transferred_bytes.clone(),
        );
        registry.register(
            "operation_duration_seconds",
            "Object-store request latency in seconds, by operation. One \
             observation per attempt.",
            this.duration.clone(),
        );

        this
    }

    /// Handles that no registry holds, so nothing they record is exported.
    ///
    /// This is for a call site that has no registry: a unit test, and the
    /// `BlockWriter` a test builds by hand. Do not use it in a service. A
    /// service that wires this instead of [`Self::register`] gets a store that
    /// looks healthy because every one of its series is absent.
    #[must_use]
    pub fn unregistered() -> Self {
        Self {
            operations: Family::default(),
            failures: Family::default(),
            retries: Family::default(),
            transferred_bytes: Family::default(),
            duration: Family::<ObjectStoreOperationLabel, Histogram>::new_with_constructor(|| {
                Histogram::new(OPERATION_DURATION_BUCKETS)
            }),
        }
    }

    /// Records one completed object-store request attempt.
    ///
    /// `elapsed` is the time the attempt took, whatever its outcome. A failed
    /// attempt is observed in the latency histogram as well, because the time
    /// an operator waits for a failure is time the caller waited.
    pub fn record_operation(&self, operation: ObjectStoreOperation, ok: bool, elapsed: Time) {
        let label = ObjectStoreOperationLabel::from(operation);
        self.operations.get_or_create(&label).inc();
        if !ok {
            self.failures.get_or_create(&label).inc();
        }
        self.duration
            .get_or_create(&label)
            .observe(elapsed.secs_f64());
    }

    /// Records that one attempt of `operation` is about to be retried.
    ///
    /// [`retry_object_store`](crate::retry_object_store) calls this after it
    /// decides a failure is transient and before it sleeps.
    pub fn record_retry(&self, operation: ObjectStoreOperation) {
        self.retries
            .get_or_create(&ObjectStoreOperationLabel::from(operation))
            .inc();
    }

    /// Records the payload `size` one request of `operation` carried.
    pub fn record_transferred(&self, operation: ObjectStoreOperation, size: ByteSize) {
        self.transferred_bytes
            .get_or_create(&ObjectStoreOperationLabel::from(operation))
            .inc_by(size.bytes_u64());
    }

    /// The attempt count for `operation`. Tests read the families through
    /// this rather than through the encoded text.
    #[must_use]
    pub fn operations(&self, operation: ObjectStoreOperation) -> u64 {
        self.operations
            .get_or_create(&ObjectStoreOperationLabel::from(operation))
            .get()
    }

    /// The failed-attempt count for `operation`.
    #[must_use]
    pub fn failures(&self, operation: ObjectStoreOperation) -> u64 {
        self.failures
            .get_or_create(&ObjectStoreOperationLabel::from(operation))
            .get()
    }

    /// The retry count for `operation`.
    #[must_use]
    pub fn retries(&self, operation: ObjectStoreOperation) -> u64 {
        self.retries
            .get_or_create(&ObjectStoreOperationLabel::from(operation))
            .get()
    }

    /// The transferred-byte total for `operation`.
    #[must_use]
    pub fn transferred_bytes(&self, operation: ObjectStoreOperation) -> u64 {
        self.transferred_bytes
            .get_or_create(&ObjectStoreOperationLabel::from(operation))
            .get()
    }
}
