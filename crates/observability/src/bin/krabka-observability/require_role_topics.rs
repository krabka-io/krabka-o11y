use krabka_client_core::ClientSecurity;
use krabka_observability::topic_contract::{LOGS_TOPICS, TopicContractError, require_topics};

use super::ServiceConfig;

/// Refuses to let this role start on topics that do not meet the contract.
///
/// A WAL topic's partition count is the write-path shard count, so a role that
/// started against the wrong one would read or write a re-mapped key space and
/// nothing on the wire would say so. The check therefore runs before the WAL
/// producer and consumer are built.
///
/// `wal_bootstrap_server` is optional because every role here also runs
/// without a WAL, reading and writing local files. With no broker named there
/// is no topic to check and nothing to refuse. `security` is the WAL client
/// security that the binary loaded, and `None` connects in plain text.
///
/// # Errors
/// Returns [`TopicContractError`] when no bootstrap address answers, or when a
/// topic is absent or not compacted.
pub(crate) async fn require_role_topics(
    config: &ServiceConfig,
    security: Option<ClientSecurity>,
) -> Result<(), TopicContractError> {
    let Some(bootstrap) = config.wal_bootstrap_server.as_deref() else {
        return Ok(());
    };
    require_topics(bootstrap, &LOGS_TOPICS, security).await?;
    Ok(())
}
