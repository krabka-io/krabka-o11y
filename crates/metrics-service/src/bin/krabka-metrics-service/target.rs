use krabka_observability::RoleKind;

use super::ValueEnum;

/// The metrics read roles and the all-in-one stack.
///
/// `all` runs a distributor, block builder, compactor and querier in one runtime.
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
#[value(rename_all = "kebab-case")]
pub(crate) enum Target {
    /// Answers a `PromQL` query over blocks and the WAL head.
    Querier,
    /// Runs the metrics write and query paths in one process.
    All,
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
            Self::All => RoleKind::All,
            Self::QueryFrontend => RoleKind::QueryFrontend,
            Self::Ruler => RoleKind::Ruler,
        }
    }
}
