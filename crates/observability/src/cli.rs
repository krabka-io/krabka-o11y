use super::*;

#[derive(Debug, Parser)]
pub(crate) struct Cli {
    #[command(flatten)]
    pub(crate) profiling: krabka_telemetry::profiling::ProfilingConfig,
    #[command(flatten)]
    pub(crate) service: ServiceConfig,
    #[arg(
        long,
        env = "KRABKA_OBSERVABILITY_CLIENT_DISPATCH_QUEUE_CAPACITY",
        default_value_t = krabka_client_core::DEFAULT_CONNECTION_DISPATCH_QUEUE_CAPACITY,
        value_parser = parse_dispatch_queue_capacity
    )]
    pub(crate) client_dispatch_queue_capacity: usize,
    #[arg(
        long,
        env = "KRABKA_OBSERVABILITY_CLIENT_FRAME_MAX",
        default_value = "100MiB",
        value_parser = parse_frame_max
    )]
    pub(crate) client_frame_max: ByteSize,
}
