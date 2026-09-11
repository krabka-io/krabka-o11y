use krabka_observability::topic_contract::TopicContractError;

use super::{Cli, METRICS_TOPICS, require_topics};

/// Refuses to let this role start on topics that do not meet the contract.
///
/// A WAL topic's partition count is the write-path shard count, so a
/// distributor that produced into a re-partitioned topic would route every
/// key to a shard that holds none of its history, and nothing on the wire
/// would say so. The check therefore runs before any producer or consumer is
/// built, and a role that reaches no broker at all skips it rather than making
/// a broker it does not use a condition of its starting.
///
/// # Errors
/// Returns [`TopicContractError`] when no bootstrap address answers, or when a
/// topic is absent or not compacted.
pub(crate) async fn require_role_topics(cli: &Cli) -> Result<(), TopicContractError> {
    if !cli.target.touches_the_wal() {
        return Ok(());
    }
    require_topics(&cli.bootstrap, &METRICS_TOPICS).await?;
    Ok(())
}
