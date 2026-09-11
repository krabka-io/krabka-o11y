use krabka_observability::topic_contract::{PROFILES_TOPICS, TopicContractError, require_topics};

use super::{Cli, Target};

/// Refuses to let this role start on topics that do not meet the contract.
///
/// A WAL topic's partition count is the write-path shard count, so a role
/// started against the wrong one would route every key to a shard that holds
/// none of its history, and nothing on the wire would say so. The check
/// therefore runs before any producer or consumer is built.
///
/// The compactor and the symbolizer reach no broker at all -- one reads and
/// writes the object store, the other resolves symbols over HTTP -- so asking
/// them to validate a topic would make a broker they do not use a condition of
/// their starting, which is a fault they do not have.
///
/// # Errors
/// Returns [`TopicContractError`] when no bootstrap address answers, or when
/// the topic is absent.
pub(crate) async fn require_role_topics(cli: &Cli) -> Result<(), TopicContractError> {
    if !cli.target.touches_the_wal() {
        return Ok(());
    }
    require_topics(&cli.bootstrap, &PROFILES_TOPICS).await?;
    Ok(())
}

impl Target {
    /// Whether this role opens a WAL client. The match is exhaustive so a new
    /// role has to answer the question rather than inherit an answer.
    ///
    /// `all` answers once for the process rather than once per role it
    /// composes: it runs four roles that open WAL clients, and asking the
    /// broker the same question four times would turn one contract violation
    /// into four indistinguishable start failures.
    pub(crate) fn touches_the_wal(self) -> bool {
        match self {
            // The querier and the query-frontend both tail the WAL for their
            // hot tier.
            Self::Distributor
            | Self::BlockBuilder
            | Self::Querier
            | Self::QueryFrontend
            | Self::All => true,
            Self::Compactor | Self::Symbolizer => false,
        }
    }
}
