/// What a service holds open for as long as it is exporting telemetry.
///
/// Dropping it is not enough: OTLP spans sit in a batch exporter that has to
/// be flushed, so a service calls [`Telemetry::shutdown`] on its way out.
pub struct Telemetry {
    pub(crate) guard: Option<krabka_telemetry::TelemetryGuard>,
}

impl Telemetry {
    /// Flushes and stops whatever exporters this process started.
    pub fn shutdown(self) {
        if let Some(guard) = self.guard {
            guard.shutdown();
        }
    }
}
