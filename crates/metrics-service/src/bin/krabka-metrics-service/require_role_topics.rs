use krabka_observability::topic_contract::{
    METRICS_SERVICE_TOPICS, TopicContractError, require_topics,
};

use super::Cli;

/// Refuses to let this role start on topics that do not meet the contract.
///
/// The metrics WAL topic's partition count is the write-path shard count, and
/// the ruler's state topic must be compacted or the broker discards every
/// pending alert at the retention window. Neither failure reports itself, so
/// the check runs before any producer or consumer is built.
///
/// `--wal-bootstrap` is optional: a querier or query-frontend serving only
/// compacted blocks reaches no broker, and making one a condition of its
/// starting would be a fault it does not have. With no bootstrap address there
/// is no topic to check. The ruler, which does require the flag, fails on its
/// own with a message naming it.
///
/// # Errors
/// Returns [`TopicContractError`] when no bootstrap address answers, or when a
/// topic is absent or not compacted.
pub(crate) async fn require_role_topics(cli: &Cli) -> Result<(), TopicContractError> {
    let Some(bootstrap) = cli.wal_bootstrap.as_deref() else {
        return Ok(());
    };
    require_topics(bootstrap, &METRICS_SERVICE_TOPICS).await?;
    Ok(())
}
