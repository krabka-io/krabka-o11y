use krabka_blockstore::DrainedGroup;

use super::DeploymentKafkaNames;

/// The default block-builder consumer group of each signal, with the WAL
/// topic it reads.
///
/// A deployment that sets another group id or WAL topic names its own groups
/// with [`DeploymentKafkaNames::drained_groups`].
#[must_use]
pub fn deployment_drained_groups() -> Vec<DrainedGroup> {
    DeploymentKafkaNames::default().drained_groups()
}
