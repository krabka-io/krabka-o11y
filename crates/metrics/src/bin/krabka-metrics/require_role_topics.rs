use krabka_observability::topic_contract::TopicContractError;

use super::{Cli, ClientSecurity, METRICS_TOPICS, require_topics};

/// Refuses to let this role start on topics that do not meet the contract.
///
/// A WAL topic's partition count is the write-path shard count, so a
/// distributor that produced into a re-partitioned topic would route every
/// key to a shard that holds none of its history, and nothing on the wire
/// would say so. The check therefore runs before any producer or consumer is
/// built.
///
/// Both roles of this binary reach the broker, so both are asked. The read
/// path, which can serve compacted blocks with no broker at all, is
/// `krabka-metrics-service`, and it makes that call for itself.
///
/// `security` is the write-ahead log client security that the binary loaded.
/// `None` connects in plain text.
///
/// # Errors
/// Returns [`TopicContractError`] when no bootstrap address answers, or when a
/// topic is absent or not compacted.
pub(crate) async fn require_role_topics(
    cli: &Cli,
    security: Option<ClientSecurity>,
) -> Result<(), TopicContractError> {
    require_topics(&cli.bootstrap, &METRICS_TOPICS, security).await?;
    Ok(())
}
