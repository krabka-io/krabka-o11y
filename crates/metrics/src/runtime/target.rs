use krabka_observability::RoleKind;

use super::ValueEnum;

/// The roles `krabka-metrics` has.
///
/// Two of these are the metrics write path and both reach the broker. The third
/// reads and writes object storage only. The read path -- the `PromQL` query
/// API, the query-frontend and the ruler -- is `krabka-metrics-service`, a
/// separate binary with its own `--target`. This binary once carried those three
/// names too, over a router that served `/api/v1/status/buildinfo` and nothing
/// else; [`retired_role_message`] is what an operator who still asks for one of
/// them now gets.
///
/// Metrics has no `live-store`. A metrics querier reads the recent window from
/// the WAL itself rather than from a separate hot tier. That is a real gap
/// rather than a difference in naming.
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
    ///
    /// It also runs the retention and orphan sweep, which upstream's compactor
    /// would run.
    BlockBuilder,
    /// Merges the blocks that are already in object storage into larger ones.
    ///
    /// This is Mimir's `compactor` on the merge half of its job. It reads the
    /// `.index` manifests, merges the blocks a level policy chooses, publishes
    /// the output and retires the inputs. It reaches no broker at all.
    Compactor,
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
            Self::Compactor => RoleKind::Compactor,
        }
    }

    /// Whether this role opens a WAL client. The match is exhaustive so a new
    /// role has to answer the question rather than inherit an answer.
    ///
    /// The compactor reaches no broker: it reads and writes the object store,
    /// and [`RoleKind::Compactor`] says so. Asking it to validate a WAL topic
    /// would make a broker it never uses a condition of its starting, which is
    /// a fault it does not have.
    pub(crate) const fn touches_the_wal(self) -> bool {
        match self {
            Self::Distributor | Self::BlockBuilder => true,
            Self::Compactor => false,
        }
    }
}
