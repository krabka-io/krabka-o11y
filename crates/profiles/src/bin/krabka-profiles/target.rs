use krabka_observability::RoleKind;

use super::ValueEnum;

/// The roles `krabka-profiles` has, and the single-process composition of
/// them.
///
/// Pyroscope is the upstream this binary matches, and it supplies most of
/// these names directly: `distributor`, `querier`, `query-frontend`,
/// `compactor` and `symbolizer` are Pyroscope targets, and `all` is what
/// Pyroscope -- like Loki, Mimir and Tempo -- calls the single-process mode.
/// [`BlockBuilder`](Self::BlockBuilder) is the one name that Pyroscope does
/// not supply: Mimir and Tempo both spell the Kafka-WAL consumer that writes
/// blocks `block-builder`, which is exactly what this role is, so it takes the
/// name two upstreams already agree on rather than inventing a third.
///
/// Two stages of the shared vocabulary are absent, and both gaps are real
/// rather than a difference in naming. There is no `live-store`, because only
/// traces keeps the window that no block covers yet as a role of its own; a
/// profiles querier tails the WAL for that window itself. There is no `ruler`,
/// because nothing evaluates recording or alerting rules over profiles.
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
#[value(rename_all = "kebab-case")]
pub(crate) enum Target {
    /// Accepts pushes at the Pyroscope ingest doors, applies the tenant's
    /// limits, and writes the profiles WAL.
    Distributor,
    /// Consumes the profiles WAL and writes blocks, their symbol databases and
    /// the index snapshot to object storage.
    BlockBuilder,
    /// Answers a query from the WAL tail it keeps and the blocks the index
    /// names.
    Querier,
    /// Answers a query by splitting its range into shards and merging what
    /// each shard returns.
    ///
    /// In this crate that is all the frontend is: a querier whose execution is
    /// sharded by `--query-frontend-shard-width` over its own store. It does
    /// not fan out over HTTP to querier replicas, which is why no querier
    /// addresses are configurable and why `--target all` has nothing to wire
    /// between the two read roles.
    QueryFrontend,
    /// Merges blocks that are already in object storage into larger ones, and
    /// downsamples them when asked to.
    Compactor,
    /// Resolves native addresses to function names through debuginfod.
    Symbolizer,
    /// Every role above, in one process.
    ///
    /// One Pyroscope-facing port, one object store, one WAL topic check and
    /// one `/ready` for the whole process. See [`run_all`](super::run_all) for
    /// what that composition shares and why sharing it is not optional.
    All,
}

impl Target {
    /// This role in the vocabulary every signal shares.
    ///
    /// The enum above is the subset `krabka-profiles` implements; [`RoleKind`]
    /// is where the names live, so that a stage is spelled the same way in
    /// every binary, in every manifest and in every runbook.
    pub(crate) const fn kind(self) -> RoleKind {
        match self {
            Self::Distributor => RoleKind::Distributor,
            Self::BlockBuilder => RoleKind::BlockBuilder,
            Self::Querier => RoleKind::Querier,
            Self::QueryFrontend => RoleKind::QueryFrontend,
            Self::Compactor => RoleKind::Compactor,
            Self::Symbolizer => RoleKind::Symbolizer,
            Self::All => RoleKind::All,
        }
    }
}
