use krabka_client_admin::AdminClient;

use super::{TopicContract, TopicContractError, TopicReport, check_topics};

/// Connects to the broker and refuses to let a role start on topics that are
/// absent or not compacted.
///
/// This is the call a service binary makes at startup, before it builds a
/// producer or joins a consumer group. It creates nothing and states no
/// partition count: the deployment step owns that number, and this reads it
/// back and logs it.
///
/// # Errors
/// Returns the error [`check_topics`] returns, and a connection error when no
/// bootstrap address answers.
pub async fn require_topics(
    bootstrap: &str,
    topics: &[TopicContract],
) -> Result<TopicReport, TopicContractError> {
    let mut admin = AdminClient::connect(&[bootstrap.to_string()]).await?;
    let report = check_topics(&mut admin, topics).await?;
    for topic in &report.observed {
        tracing::info!(
            topic = %topic.name,
            shards = %topic.partitions,
            replication_factor = topic.replication_factor,
            "topic ready",
        );
    }
    Ok(report)
}
