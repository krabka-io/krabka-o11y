use krabka_client_admin::AdminClient;

use super::{
    TopicContract, TopicContractError, TopicDrift, TopicExpectation, TopicReport, TopicSettings,
    desired_specs, inspect_topics,
};

/// Describes the topics and compares them against the whole contract,
/// deployment numbers included. Creates nothing.
///
/// Only the deployment step knows the partition count it asked for, so only
/// the deployment step checks it. A role calls [`check_topics`] instead and
/// reads the live count out of the report.
///
/// # Errors
/// Returns [`TopicContractError::Contract`] when a topic is absent,
/// unreadable, has the wrong partition count, or is a state topic that is not
/// compacted. Advisory differences come back inside the report instead.
pub async fn verify_topics(
    admin: &mut AdminClient,
    topics: &[TopicContract],
    settings: &TopicSettings,
) -> Result<TopicReport, TopicContractError> {
    settings.validate()?;
    let specs = desired_specs(topics, settings);
    report(admin, topics, &TopicExpectation::Deployment(&specs)).await
}

/// Describes the topics and checks what holds whatever the deployment's size:
/// the topic exists, and a state topic is compacted.
///
/// This is the check a role runs at startup, including a role that only
/// reads. A querier that reads a WAL written under a different shard count
/// reads a re-mapped key space, so "read-only" is not "exempt" -- but the
/// number it needs is in [`TopicReport::partitions`], not in its own
/// configuration.
///
/// # Errors
/// Returns [`TopicContractError::Contract`] when a topic is absent,
/// unreadable, or is a state topic that is not compacted.
pub async fn check_topics(
    admin: &mut AdminClient,
    topics: &[TopicContract],
) -> Result<TopicReport, TopicContractError> {
    report(admin, topics, &TopicExpectation::Structure).await
}

async fn report(
    admin: &mut AdminClient,
    topics: &[TopicContract],
    expectation: &TopicExpectation<'_>,
) -> Result<TopicReport, TopicContractError> {
    let names: Vec<&str> = topics.iter().map(|topic| topic.name).collect();
    let metadata = admin.metadata(&names).await?;
    let configs = admin.describe_configs(&names).await?;
    let (observed, drift) = inspect_topics(topics, expectation, &metadata, &configs);

    let (fatal, advisory): (Vec<_>, Vec<_>) = drift.into_iter().partition(TopicDrift::is_fatal);
    if !fatal.is_empty() {
        return Err(TopicContractError::Contract { drift: fatal });
    }
    Ok(TopicReport { observed, advisory })
}
