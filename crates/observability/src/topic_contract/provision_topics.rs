use krabka_client_admin::AdminClient;
use krabka_client_core::ClientSecurity;

use super::{TopicContract, TopicContractError, TopicReport, TopicSettings, ensure_topics};

/// Connects to the broker under `security`, provisions the named topics, and
/// reports what it found.
///
/// This is the call a deployment step makes before the roles start. It logs
/// the live shard count for each topic and warns about every advisory
/// difference, so an under-replicated or short-retention topic is visible
/// without stopping the role. `None` connects in plain text.
///
/// # Errors
/// Returns the error [`ensure_topics`] returns, and a connection error when
/// no bootstrap address answers.
pub async fn provision_topics(
    bootstrap: &str,
    topics: &[TopicContract],
    settings: &TopicSettings,
    security: Option<ClientSecurity>,
) -> Result<TopicReport, TopicContractError> {
    let mut admin = AdminClient::connect_secured(&[bootstrap.to_string()], security).await?;
    let report = ensure_topics(&mut admin, topics, settings).await?;
    for topic in &report.observed {
        tracing::info!(
            topic = %topic.name,
            shards = %topic.partitions,
            replication_factor = topic.replication_factor,
            "topic ready",
        );
    }
    for drift in &report.advisory {
        tracing::warn!(topic = %drift.topic(), "{drift}");
    }
    Ok(report)
}
