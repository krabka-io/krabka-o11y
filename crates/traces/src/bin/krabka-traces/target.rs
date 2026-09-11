use krabka_observability::RoleKind;

use super::ValueEnum;

/// The roles `krabka-traces` has.
///
/// Traces carries two stages the other signals do not, and both are real
/// differences rather than accidents of this binary. [`LiveStore`] exists
/// because traces is the only signal with a separate hot tier: a logs or
/// metrics querier tails the WAL itself for the window no block covers yet,
/// whereas a trace has to be assembled from spans that arrive over seconds and
/// is therefore held, and queried, in a role of its own.
/// [`MetricsGenerator`] exists because traces is the only signal that derives
/// another signal from itself -- span metrics and a service graph, remote-
/// written to a Prometheus endpoint.
///
/// The spellings are not this repository's to choose. Tempo 3.0 and later
/// names these stages `block-builder`, `live-store`, `metrics-generator`,
/// `querier`, `query-frontend`, `distributor` and `all`, and an operator
/// arriving from Tempo types those. The one name Tempo does not supply is
/// [`Compactor`]: Mimir's word for the job that merges blocks already in
/// object storage into larger ones, which is exactly what this crate's
/// compactor does and is why it carries Mimir's name for it.
///
/// [`LiveStore`]: Self::LiveStore
/// [`MetricsGenerator`]: Self::MetricsGenerator
/// [`Compactor`]: Self::Compactor
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
#[value(rename_all = "kebab-case")]
pub(crate) enum Target {
    Distributor,
    BlockBuilder,
    LiveStore,
    Querier,
    QueryFrontend,
    Compactor,
    MetricsGenerator,
    /// Every role above, in one process. See
    /// [`run_all`](super::run_all::run_all) for what it composes and in what
    /// order it stops.
    All,
}

impl Target {
    /// The cross-signal name of this role.
    ///
    /// This enum is the subset of the stack's stages that traces implements,
    /// and it exists because clap needs an enum to parse `--target` into.
    /// [`RoleKind`] is the vocabulary itself: it spans all four signals, and
    /// its `as_str` is the one spelling an operator, a manifest and a
    /// readiness gate all use. Mapping one onto the other here -- rather than
    /// letting two independent kebab-case derives happen to agree -- is what
    /// keeps a rename of a stage from silently forking into a traces spelling
    /// and an everything-else spelling.
    pub(crate) const fn kind(self) -> RoleKind {
        match self {
            Self::Distributor => RoleKind::Distributor,
            Self::BlockBuilder => RoleKind::BlockBuilder,
            Self::LiveStore => RoleKind::LiveStore,
            Self::Querier => RoleKind::Querier,
            Self::QueryFrontend => RoleKind::QueryFrontend,
            Self::Compactor => RoleKind::Compactor,
            Self::MetricsGenerator => RoleKind::MetricsGenerator,
            Self::All => RoleKind::All,
        }
    }
}
