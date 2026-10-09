use super::{
    AuditArgs, Parser, ServerSecurityArgs, SocketAddr, Target, TargetValueParser,
    WalClientSecurityArgs, WriterConfig,
};

/// Configuration for a standalone metrics writer process.
#[derive(Debug, Parser)]
pub struct Cli {
    #[command(flatten)]
    pub(crate) writer: WriterConfig,
    #[command(flatten)]
    pub(crate) profiling: krabka_telemetry::profiling::ProfilingConfig,
    /// The role this process runs: `distributor`, `block-builder` or
    /// `compactor`.
    ///
    /// The read-path roles are `krabka-metrics-service`'s, not this binary's,
    /// and naming one here is rejected with a message that says so.
    #[arg(long, env = "KRABKA_METRICS_TARGET", value_parser = TargetValueParser)]
    pub(crate) target: Target,
    /// Address for the admin port: pprof, Prometheus metrics and `/ready`. Default: `0.0.0.0:9404`.
    ///
    /// The admin port always serves plain HTTP with no authentication. The
    /// TLS and credential flags apply only to the data port. Bind the admin
    /// port to an address that only the cluster can reach.
    #[arg(long, env = "KRABKA_ADMIN_LISTEN_ADDR", default_value = "0.0.0.0:9404")]
    pub(crate) admin_listen_addr: SocketAddr,
    // The shared security flags come after this binary's own flags, so
    // `--help` lists the role and its listener first.
    /// TLS, authentication and outbound-credential flags for the data port.
    #[command(flatten)]
    pub(crate) server_security: ServerSecurityArgs,
    /// Audit trail flags. The audit layer is off until `--audit-topic` is set.
    #[command(flatten)]
    pub(crate) audit: AuditArgs,
    /// TLS and SASL flags for every broker connection, the audit producer included.
    #[command(flatten)]
    pub(crate) wal_security: WalClientSecurityArgs,
}

impl std::ops::Deref for Cli {
    type Target = WriterConfig;
    fn deref(&self) -> &Self::Target {
        &self.writer
    }
}

impl std::ops::DerefMut for Cli {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.writer
    }
}
