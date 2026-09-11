use krabka_client_admin::AdminClient;

use super::{TopicContract, TopicContractError, TopicReport, TopicSettings, ensure_topics};

/// Connects to the broker, provisions the named topics, and reports what it
/// found.
///
/// This is the call a service binary makes at startup, before it builds a
/// producer or joins a consumer group. It logs the live shard count for each
/// topic and warns about every advisory difference, so an under-replicated or
/// short-retention topic is visible without stopping the role.
///
/// # Errors
/// Returns the error [`ensure_topics`] returns, and a connection error when
/// no bootstrap address answers.
pub async fn provision_topics(
    bootstrap: &str,
    topics: &[TopicContract],
    settings: &TopicSettings,
) -> Result<TopicReport, TopicContractError> {
    let mut admin = AdminClient::connect(&[bootstrap.to_string()]).await?;
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
