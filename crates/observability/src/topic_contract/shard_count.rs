use krabka_client_admin::AdminClient;

use super::{PartitionCount, TopicContractError, TopicDrift};

/// Reads a topic's live shard count from broker metadata.
///
/// This is the only way to learn the write-path shard count, and it is
/// deliberate that no setting supplies it. The producer routes a keyed record
/// with `murmur2(key) % partition_count` against the partition count it reads
/// from the same metadata, so a number kept anywhere else could only ever
/// disagree with the one that decides where records land.
///
/// # Errors
/// Returns [`TopicContractError::Contract`] when the topic is absent or the
/// broker refuses to report it.
pub async fn shard_count(
    admin: &mut AdminClient,
    topic: &str,
) -> Result<PartitionCount, TopicContractError> {
    let metadata = admin.metadata(&[topic]).await?;
    let missing = || TopicContractError::Contract {
        drift: vec![TopicDrift::Missing {
            topic: topic.to_string(),
            purpose: "the write-path shard count for this signal",
        }],
    };
    let entry = metadata
        .topics
        .iter()
        .find(|entry| entry.name == topic)
        .ok_or_else(missing)?;
    if let Some(error) = &entry.error {
        return Err(TopicContractError::Contract {
            drift: vec![TopicDrift::Unreadable {
                topic: topic.to_string(),
                error: error.name.to_string(),
            }],
        });
    }
    PartitionCount::new(entry.partition_count).map_err(|_| missing())
}
