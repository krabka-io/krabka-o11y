use super::{RoleKind, ValueEnum};

/// The roles `krabka-observability` can run.
///
/// Logs has three of them, and the two stages it does not have are absent for
/// reasons rather than by oversight. There is no `live-store`: a logs querier
/// tails the WAL itself, so the recent window is served by the same role that
/// serves the blocks. There is no separate `compactor` either -- `Loki` folds
/// index compaction, retention and delete-request materialisation into the
/// stage that writes durable storage, and so does
/// [`BlockBuilder`](Self::BlockBuilder).
#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum Role {
    /// Accepts `Loki` and OTLP pushes and writes the logs WAL.
    Distributor,
    /// Consumes the logs WAL, writes blocks to object storage, and
    /// materialises delete requests as it goes.
    BlockBuilder,
    /// Serves the `Loki` query API over the blocks and the WAL tail.
    Querier,
    /// All three, in one process.
    All,
}

impl Role {
    /// This role in the vocabulary every signal shares.
    #[must_use]
    pub const fn kind(self) -> RoleKind {
        match self {
            Self::Distributor => RoleKind::Distributor,
            Self::BlockBuilder => RoleKind::BlockBuilder,
            Self::Querier => RoleKind::Querier,
            Self::All => RoleKind::All,
        }
    }
}
