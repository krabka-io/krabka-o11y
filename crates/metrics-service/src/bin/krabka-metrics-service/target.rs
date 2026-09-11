use krabka_observability::RoleKind;

use super::ValueEnum;

/// The roles `krabka-metrics-service` has.
///
/// These three are the metrics read path, and this binary is the only one that
/// has them. The write path -- ingest and block building -- is
/// `krabka-metrics`, whose `--target` takes `distributor` and `block-builder`
/// and refuses each of these by name.
///
/// There is no `all` here for the same reason: metrics is the one signal whose
/// roles are split across two binaries, so no single process can run them.
/// Logs, traces and profiles each have a `--target all`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
#[value(rename_all = "kebab-case")]
pub(crate) enum Target {
    /// Answers a `PromQL` query over blocks and the WAL head.
    Querier,
    /// Shards a query, fans out to queriers, and merges what comes back.
    QueryFrontend,
    /// Evaluates recording and alerting rules.
    Ruler,
}

impl Target {
    /// This role in the vocabulary every signal shares.
    ///
    /// The enum above is the subset this binary implements; [`RoleKind`] is
    /// where the names live, so that a stage is spelled the same way in every
    /// binary and in every manifest.
    pub(crate) const fn kind(self) -> RoleKind {
        match self {
            Self::Querier => RoleKind::Querier,
            Self::QueryFrontend => RoleKind::QueryFrontend,
            Self::Ruler => RoleKind::Ruler,
        }
    }
}
