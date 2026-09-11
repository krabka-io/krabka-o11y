use std::{
    fmt,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

use krabka_audit::{AuditLog, AuditStats};
use qubit_clock::{Clock, SystemClock};

use super::{
    AuditEndpoint, AuditEvent, AuditOutcome, AuditPrincipal, AuditResource, EpochMs,
    admin_operation, authentication, authorization_denied,
};

/// The cheap, cloneable handle that request handlers record audit events
/// through.
///
/// Every method returns at once. [`emit`](Self::emit) puts the event in a
/// bounded queue and does not wait for the writer. When the queue is full, the
/// handle drops the event and counts the drop in [`dropped`](Self::dropped).
/// A request therefore never waits for the audit topic, and a slow broker
/// does not make a request slow.
///
/// A disabled handle discards every event and counts nothing.
#[derive(Clone)]
pub struct AuditHandle {
    inner: Arc<Inner>,
}

struct Inner {
    log: Arc<AuditLog>,
    stats: Arc<AuditStats>,
    clock: Arc<dyn Clock>,
    enabled: bool,
    closed: AtomicBool,
    dropped_after_close: AtomicU64,
}

impl AuditHandle {
    /// A handle that discards every event.
    ///
    /// A service with no `--audit-topic` uses this handle.
    #[must_use]
    pub fn disabled() -> Self {
        Self::build(
            AuditLog::disabled(),
            Arc::new(AuditStats::new()),
            Arc::new(SystemClock::new()),
            false,
        )
    }

    /// An enabled handle over `log`, whose receiver the caller drains.
    ///
    /// [`AuditService`](super::AuditService) gives `log` to the writer and
    /// `stats` to the writer. A test can drain the receiver itself. The clock
    /// gives each event its time.
    #[must_use]
    pub fn new(log: Arc<AuditLog>, stats: Arc<AuditStats>, clock: Arc<dyn Clock>) -> Self {
        Self::build(log, stats, clock, true)
    }

    fn build(
        log: Arc<AuditLog>,
        stats: Arc<AuditStats>,
        clock: Arc<dyn Clock>,
        enabled: bool,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                log,
                stats,
                clock,
                enabled,
                closed: AtomicBool::new(false),
                dropped_after_close: AtomicU64::new(0),
            }),
        }
    }

    /// Whether this handle records events.
    #[must_use]
    pub fn is_enabled(&self) -> bool {
        self.inner.enabled
    }

    /// The current time from the handle's clock.
    #[must_use]
    pub fn now(&self) -> EpochMs {
        EpochMs(self.inner.clock.millis())
    }

    /// Puts `event` in the queue to the writer, and does not wait.
    ///
    /// The handle drops the event and counts the drop when the queue is full,
    /// and when the service has stopped the writer.
    pub fn emit(&self, event: AuditEvent) {
        let inner = &*self.inner;
        if !inner.enabled {
            return;
        }
        if inner.closed.load(Ordering::SeqCst) {
            inner.dropped_after_close.fetch_add(1, Ordering::Relaxed);
            tracing::warn!("audit event dropped: the audit writer has stopped");
            return;
        }
        inner.log.emit(event);
    }

    /// Records an operation that changes a tenant's data or the service, at
    /// the current time.
    ///
    /// See [`admin_operation`](super::admin_operation) for the arguments.
    pub fn admin_operation(
        &self,
        principal: AuditPrincipal,
        source: AuditEndpoint,
        operation: &'static str,
        resources: Vec<AuditResource>,
        outcome: AuditOutcome,
    ) {
        self.emit(admin_operation(
            principal,
            source,
            operation,
            resources,
            outcome,
            self.now(),
        ));
    }

    /// Records a refused request, at the current time.
    ///
    /// See [`authorization_denied`](super::authorization_denied) for the
    /// arguments.
    pub fn authorization_denied(
        &self,
        principal: AuditPrincipal,
        source: AuditEndpoint,
        resource_type: &'static str,
        resource_name: impl Into<String>,
        operation: &'static str,
    ) {
        self.emit(authorization_denied(
            principal,
            source,
            resource_type,
            resource_name,
            operation,
            self.now(),
        ));
    }

    /// Records an authentication attempt, at the current time.
    ///
    /// See [`authentication`](super::authentication) for the arguments.
    pub fn authentication(
        &self,
        outcome: AuditOutcome,
        mechanism: &'static str,
        principal: AuditPrincipal,
        source: AuditEndpoint,
        reason: Option<String>,
    ) {
        self.emit(authentication(
            outcome,
            mechanism,
            principal,
            source,
            reason,
            self.now(),
        ));
    }

    /// The count of events that did not reach the audit topic and are not in
    /// the spool.
    ///
    /// The count adds the events that the full queue dropped, the events
    /// emitted after the writer stopped, and the records that the writer
    /// dropped because the topic refused them and the spool was full or
    /// absent. A service should export this count as a counter.
    #[must_use]
    pub fn dropped(&self) -> u64 {
        let inner = &*self.inner;
        inner
            .log
            .dropped()
            .saturating_add(inner.stats.dropped())
            .saturating_add(inner.dropped_after_close.load(Ordering::Relaxed))
    }

    /// The writer's spool counters: records spooled, records replayed, and
    /// the current spool depth and size.
    #[must_use]
    pub fn stats(&self) -> &AuditStats {
        &self.inner.stats
    }

    /// Stops the queue to the writer.
    ///
    /// The writer writes the events already in the queue, then stops. The
    /// handle counts each later event as dropped.
    pub(crate) fn close(&self) {
        self.inner.closed.store(true, Ordering::SeqCst);
        self.inner.log.close();
    }
}

impl fmt::Debug for AuditHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuditHandle")
            .field("enabled", &self.inner.enabled)
            .field("dropped", &self.dropped())
            .finish_non_exhaustive()
    }
}
