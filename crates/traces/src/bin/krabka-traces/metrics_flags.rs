use super::*;

#[derive(Debug, Args)]
pub(crate) struct MetricsFlags {
    #[arg(long, env = "KRABKA_TRACES_ENABLE_TARGET_INFO")]
    pub(crate) enable_target_info: bool,
    #[arg(long, env = "KRABKA_TRACES_ENABLE_STATUS_MESSAGE")]
    pub(crate) enable_status_message: bool,
    #[arg(long, env = "KRABKA_TRACES_ENABLE_MESSAGING_SYSTEM_LATENCY")]
    pub(crate) enable_messaging_system_latency: bool,
}
