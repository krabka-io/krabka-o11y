use krabka_observability::RoleKind;

use super::ValueEnum;

/// The roles `krabka-metrics` has.
///
/// Both of these are the metrics write path, and both reach the broker. The
/// read path -- the `PromQL` query API, the query-frontend and the ruler -- is
/// `krabka-metrics-service`, a separate binary with its own `--target`. This
/// binary once carried those three names too, over a router that served
/// `/api/v1/status/buildinfo` and nothing else; [`retired_role_message`] is
/// what an operator who still asks for one of them now gets.
///
/// Metrics has no `live-store` and no `compactor` of its own. A metrics
/// querier reads the recent window from the WAL itself rather than from a
/// separate hot tier, and nothing yet merges metrics blocks that are already
/// in object storage -- retention is swept from inside the block builder. Both
/// are real gaps rather than differences in naming.
///
/// [`retired_role_message`]: super::retired_role_message
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
#[value(rename_all = "kebab-case")]
pub(crate) enum Target {
    /// Accepts remote-write and OTLP pushes and writes the metrics WAL.
    Distributor,
    /// Consumes the metrics WAL and writes blocks to object storage.
    ///
    /// This role was called `compactor` until the role vocabulary was settled
    /// across the four signals. It never compacted anything: it reads the WAL
    /// and writes blocks, which is what Mimir and Tempo both call a
    /// `block-builder`, while `compactor` in all four upstreams means the job
    /// that merges blocks already in object storage. One word on two jobs is a
    /// hazard in a runbook, so this half took the name upstream agrees on.
    BlockBuilder,
}

impl Target {
    /// This role in the vocabulary every signal shares.
    ///
    /// The enum above is the subset `krabka-metrics` implements;
    /// [`RoleKind`] is where the names live, so that a stage is spelled the
    /// same way in every binary and in every manifest.
    pub(crate) const fn kind(self) -> RoleKind {
        match self {
            Self::Distributor => RoleKind::Distributor,
            Self::BlockBuilder => RoleKind::BlockBuilder,
        }
    }
}
