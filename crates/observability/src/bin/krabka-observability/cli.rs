use super::{
    ByteSize, ConfigFileArgs, Parser, ServiceConfig, SocketAddr, parse_dispatch_queue_capacity,
    parse_frame_max,
};

#[derive(Debug, Parser)]
pub(crate) struct Cli {
    #[command(flatten)]
    pub(crate) config_file: ConfigFileArgs,
    #[command(flatten)]
    pub(crate) profiling: krabka_telemetry::profiling::ProfilingConfig,
    #[command(flatten)]
    pub(crate) service: ServiceConfig,
    /// Address for the admin port: pprof, Prometheus metrics and `/ready`.
    /// Default: `0.0.0.0:9404`.
    ///
    /// The admin port serves plain HTTP with no authentication, whatever the
    /// server security flags say. Bind it to a private address.
    #[arg(long, env = "KRABKA_ADMIN_LISTEN_ADDR", default_value = "0.0.0.0:9404")]
    pub(crate) admin_listen_addr: SocketAddr,
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
