//! Kafka topic provisioning for the observability stack.

use std::collections::BTreeMap;

use krabka_client_admin::{AdminClient, CreateTopicSpec};
use krabka_units::secs;
use thiserror::Error;

const TOPIC_ALREADY_EXISTS: i16 = 36;

/// The six topics that the observability services use.
pub const TOPICS: [(&str, bool); 6] = [
    ("__krabka_metrics_wal", false),
    ("__krabka_traces_wal", false),
    ("__krabka_profiles_wal", false),
    ("__krabka_observability_logs_wal", false),
    ("__krabka_metrics_ha", true),
    ("__krabka_metrics_ruler_state", true),
];

/// A topic does not match the observability contract.
#[derive(Debug, Error)]
pub enum TopicContractError {
    #[error("admin client: {0}")]
    Admin(#[from] krabka_client_admin::AdminError),
    #[error("topic {topic}: {message}")]
    Invalid { topic: String, message: String },
}

/// Returns the canonical topic specifications.
#[must_use]
pub fn topic_specs(partitions: i32, replicas: i32, retention_ms: i64) -> Vec<CreateTopicSpec> {
    TOPICS
        .iter()
        .map(|&(name, compacted)| {
            let mut configs =
                BTreeMap::from([("retention.ms".to_string(), retention_ms.to_string())]);
            if compacted {
                configs.insert("cleanup.policy".to_string(), "compact".to_string());
            }
            CreateTopicSpec {
                name: name.to_string(),
                partitions,
                replicas,
                configs,
            }
        })
        .collect()
}

/// Creates missing topics and rejects incompatible existing topics.
///
/// # Errors
/// Returns an error when the broker rejects an operation or a topic does not match the contract.
pub async fn bootstrap_topics(
    bootstrap: &str,
    partitions: i32,
    replicas: i32,
    retention_ms: i64,
) -> Result<(), TopicContractError> {
    let specs = topic_specs(partitions, replicas, retention_ms);
    let mut admin = AdminClient::connect(&[bootstrap.to_string()]).await?;
    for outcome in admin.create_topics(&specs, secs(30)).await? {
        if let Some(error) = outcome.error
            && error.code != TOPIC_ALREADY_EXISTS
        {
            return Err(TopicContractError::Invalid {
                topic: outcome.name,
                message: error.name.to_string(),
            });
        }
    }

    let names = TOPICS.map(|(name, _)| name);
    let metadata = admin.metadata(&names).await?;
    let configs = admin.describe_configs(&names).await?;
    for spec in &specs {
        let topic = metadata
            .topics
            .iter()
            .find(|topic| topic.name == spec.name)
            .ok_or_else(|| TopicContractError::Invalid {
                topic: spec.name.clone(),
                message: "metadata is missing".to_string(),
            })?;
        if let Some(error) = &topic.error {
            return Err(TopicContractError::Invalid {
                topic: spec.name.clone(),
                message: error.name.to_string(),
            });
        }
        if topic.partition_count != spec.partitions {
            return Err(TopicContractError::Invalid {
                topic: spec.name.clone(),
                message: format!(
                    "has {} partitions, expected {}",
                    topic.partition_count, spec.partitions
                ),
            });
        }
        if topic.replication_factor != spec.replicas {
            return Err(TopicContractError::Invalid {
                topic: spec.name.clone(),
                message: format!(
                    "has replication factor {}, expected {}",
                    topic.replication_factor, spec.replicas
                ),
            });
        }
        let actual = configs
            .iter()
            .find(|config| config.topic == spec.name)
            .ok_or_else(|| TopicContractError::Invalid {
                topic: spec.name.clone(),
                message: "configuration is missing".to_string(),
            })?;
        for (key, expected) in &spec.configs {
            if actual.overrides.get(key) != Some(expected) {
                return Err(TopicContractError::Invalid {
                    topic: spec.name.clone(),
                    message: format!("{key} is not {expected}"),
                });
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    #[test]
    fn topic_specs_keep_wal_order_and_compacted_state() {
        let specs = super::topic_specs(3, 2, 900_000);
        assert!(specs.len() == 6);
        for spec in &specs[..4] {
            assert!(spec.partitions == 3);
            assert!(spec.replicas == 2);
            assert!(spec.configs.get("retention.ms") == Some(&"900000".to_string()));
            assert!(!spec.configs.contains_key("cleanup.policy"));
        }
        for spec in &specs[4..] {
            assert!(spec.configs.get("cleanup.policy") == Some(&"compact".to_string()));
        }
    }
}
