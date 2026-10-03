use clap::{Args, builder::NonEmptyStringValueParser};
use krabka_blockstore::DrainedGroup;

use crate::topic_contract::{
    LOGS_WAL_TOPIC, METRICS_HA_TOPIC, METRICS_RULER_STATE_TOPIC, METRICS_WAL_TOPIC,
    PROFILES_WAL_TOPIC, TRACES_WAL_TOPIC,
};

/// The default block-builder group of the metrics roles.
const METRICS_BLOCK_BUILDER_GROUP: &str = "krabka-metrics-block-builder";
/// The default block-builder group of the logs roles.
const LOGS_BLOCK_BUILDER_GROUP: &str = "krabka-observability-block-builder";
/// The default block-builder group of the profiles roles.
const PROFILES_BLOCK_BUILDER_GROUP: &str = "krabka-profiles-block-builder";
/// The block-builder group of the traces roles. No flag changes it.
const TRACES_BLOCK_BUILDER_GROUP: &str = "krabka-traces-block-builder";

/// The topic and block-builder group names that a deployment sets.
///
/// Each flag reads the environment variable of the role setting it mirrors,
/// and has the default of that role. The traces roles, the metrics
/// distributor, and the metrics block builder have no topic setting, so the
/// cut always holds `__krabka_traces_wal` and `__krabka_metrics_wal`.
#[derive(Args, Clone, Debug, Eq, PartialEq)]
pub struct DeploymentKafkaNames {
    /// The metrics-service `--wal-topic`. The metrics querier reads it and the
    /// ruler writes recording-rule samples to it.
    #[arg(
        long,
        env = "KRABKA_METRICS_WAL_TOPIC",
        default_value = METRICS_WAL_TOPIC,
        value_parser = NonEmptyStringValueParser::new()
    )]
    pub metrics_wal_topic: String,
    /// The metrics `--ha-tracker-topic`.
    #[arg(
        long,
        env = "KRABKA_METRICS_HA_TRACKER_TOPIC",
        default_value = METRICS_HA_TOPIC,
        value_parser = NonEmptyStringValueParser::new()
    )]
    pub metrics_ha_tracker_topic: String,
    /// The metrics-service `--ruler-state-topic`.
    #[arg(
        long,
        env = "KRABKA_METRICS_RULER_STATE_TOPIC",
        default_value = METRICS_RULER_STATE_TOPIC,
        value_parser = NonEmptyStringValueParser::new()
    )]
    pub metrics_ruler_state_topic: String,
    /// The logs `--wal-topic`.
    #[arg(
        long,
        env = "KRABKA_OBSERVABILITY_WAL_TOPIC",
        default_value = LOGS_WAL_TOPIC,
        value_parser = NonEmptyStringValueParser::new()
    )]
    pub logs_wal_topic: String,
    /// The profiles `--wal-topic`.
    #[arg(
        long,
        env = "KRABKA_PROFILES_WAL_TOPIC",
        default_value = PROFILES_WAL_TOPIC,
        value_parser = NonEmptyStringValueParser::new()
    )]
    pub profiles_wal_topic: String,
    /// The metrics `--block-builder-group-id`.
    #[arg(
        long,
        env = "KRABKA_METRICS_BLOCK_BUILDER_GROUP_ID",
        default_value = METRICS_BLOCK_BUILDER_GROUP,
        value_parser = NonEmptyStringValueParser::new()
    )]
    pub metrics_block_builder_group_id: String,
    /// The logs `--wal-group-id`, which the logs block builder joins.
    #[arg(
        long,
        env = "KRABKA_OBSERVABILITY_WAL_GROUP_ID",
        default_value = LOGS_BLOCK_BUILDER_GROUP,
        value_parser = NonEmptyStringValueParser::new()
    )]
    pub logs_wal_group_id: String,
    /// The profiles `--block-builder-group-id`.
    #[arg(
        long,
        env = "KRABKA_PROFILES_BLOCK_BUILDER_GROUP_ID",
        default_value = PROFILES_BLOCK_BUILDER_GROUP,
        value_parser = NonEmptyStringValueParser::new()
    )]
    pub profiles_block_builder_group_id: String,
}

impl Default for DeploymentKafkaNames {
    fn default() -> Self {
        Self {
            metrics_wal_topic: METRICS_WAL_TOPIC.into(),
            metrics_ha_tracker_topic: METRICS_HA_TOPIC.into(),
            metrics_ruler_state_topic: METRICS_RULER_STATE_TOPIC.into(),
            logs_wal_topic: LOGS_WAL_TOPIC.into(),
            profiles_wal_topic: PROFILES_WAL_TOPIC.into(),
            metrics_block_builder_group_id: METRICS_BLOCK_BUILDER_GROUP.into(),
            logs_wal_group_id: LOGS_BLOCK_BUILDER_GROUP.into(),
            profiles_block_builder_group_id: PROFILES_BLOCK_BUILDER_GROUP.into(),
        }
    }
}

impl DeploymentKafkaNames {
    /// Every topic that the cut holds, sorted and unique.
    #[must_use]
    pub fn topics(&self) -> Vec<String> {
        let mut topics = vec![
            METRICS_WAL_TOPIC.to_string(),
            TRACES_WAL_TOPIC.to_string(),
            self.metrics_wal_topic.clone(),
            self.metrics_ha_tracker_topic.clone(),
            self.metrics_ruler_state_topic.clone(),
            self.logs_wal_topic.clone(),
            self.profiles_wal_topic.clone(),
        ];
        topics.sort();
        topics.dedup();
        topics
    }

    /// The block-builder group of each signal, with the WAL topic it reads.
    ///
    /// These are the groups that write blocks. Each one must have committed
    /// every record of its WAL before a deployment cut is sealed.
    #[must_use]
    pub fn drained_groups(&self) -> Vec<DrainedGroup> {
        [
            (
                self.metrics_block_builder_group_id.as_str(),
                METRICS_WAL_TOPIC,
            ),
            (
                self.logs_wal_group_id.as_str(),
                self.logs_wal_topic.as_str(),
            ),
            (
                self.profiles_block_builder_group_id.as_str(),
                self.profiles_wal_topic.as_str(),
            ),
            (TRACES_BLOCK_BUILDER_GROUP, TRACES_WAL_TOPIC),
        ]
        .into_iter()
        .map(|(group, topic)| DrainedGroup {
            group: group.into(),
            topic: topic.into(),
        })
        .collect()
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    fn group(group: &str, topic: &str) -> DrainedGroup {
        DrainedGroup {
            group: group.into(),
            topic: topic.into(),
        }
    }

    #[test]
    fn the_defaults_name_the_six_contract_topics_and_the_four_block_builders() {
        let names = DeploymentKafkaNames::default();
        check!(
            names.topics()
                == [
                    "__krabka_metrics_ha",
                    "__krabka_metrics_ruler_state",
                    "__krabka_metrics_wal",
                    "__krabka_observability_logs_wal",
                    "__krabka_profiles_wal",
                    "__krabka_traces_wal",
                ]
        );
        check!(
            names.drained_groups()
                == [
                    group("krabka-metrics-block-builder", "__krabka_metrics_wal"),
                    group(
                        "krabka-observability-block-builder",
                        "__krabka_observability_logs_wal"
                    ),
                    group("krabka-profiles-block-builder", "__krabka_profiles_wal"),
                    group("krabka-traces-block-builder", "__krabka_traces_wal"),
                ]
        );
    }

    #[test]
    fn custom_names_replace_the_defaults_they_configure() {
        let names = DeploymentKafkaNames {
            metrics_wal_topic: "metrics-a".into(),
            metrics_ha_tracker_topic: "ha-a".into(),
            metrics_ruler_state_topic: "ruler-a".into(),
            logs_wal_topic: "logs-a".into(),
            profiles_wal_topic: "profiles-a".into(),
            metrics_block_builder_group_id: "metrics-builder-a".into(),
            logs_wal_group_id: "logs-builder-a".into(),
            profiles_block_builder_group_id: "profiles-builder-a".into(),
        };
        // The metrics distributor and block builder still use the default
        // metrics WAL, and the traces roles have no topic setting.
        check!(
            names.topics()
                == [
                    "__krabka_metrics_wal",
                    "__krabka_traces_wal",
                    "ha-a",
                    "logs-a",
                    "metrics-a",
                    "profiles-a",
                    "ruler-a",
                ]
        );
        check!(
            names.drained_groups()
                == [
                    group("metrics-builder-a", "__krabka_metrics_wal"),
                    group("logs-builder-a", "logs-a"),
                    group("profiles-builder-a", "profiles-a"),
                    group("krabka-traces-block-builder", "__krabka_traces_wal"),
                ]
        );
    }
}
