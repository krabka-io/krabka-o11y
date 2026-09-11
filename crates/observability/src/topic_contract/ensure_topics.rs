use krabka_client_admin::AdminClient;
use krabka_units::secs;
use tokio::time::{Duration, sleep};

use super::{
    TopicContract, TopicContractError, TopicDrift, TopicReport, TopicSettings, desired_specs,
    verify_topics,
};

/// Kafka's `TOPIC_ALREADY_EXISTS`.
const TOPIC_ALREADY_EXISTS: i16 = 36;

/// How long the broker may take to apply a `CreateTopics`.
const CREATE_TIMEOUT_SECS: u32 = 30;

/// How many times to re-describe while a freshly created topic is still
/// propagating to the broker that answers `Metadata`.
const METADATA_ATTEMPTS: usize = 10;

/// The pause between those attempts.
const METADATA_BACKOFF: Duration = Duration::from_millis(250);

/// Creates any missing topic, then describes every topic and compares it
/// against the contract.
///
/// Creation and validation are separate steps on purpose. `CreateTopics` on a
/// topic that already exists returns `TOPIC_ALREADY_EXISTS` and changes
/// nothing, so a topic that was created earlier with one partition keeps its
/// one partition however many times this runs. Only the describe finds that.
///
/// Several roles start at once in a real deployment and all of them call this.
/// That race is expected and safe: exactly one `CreateTopics` wins, and every
/// loser reads `TOPIC_ALREADY_EXISTS`, which this treats as success and then
/// validates like any other pre-existing topic.
///
/// # Errors
/// Returns [`TopicContractError::Create`] when the broker refuses to create a
/// topic for a reason other than "it already exists", and
/// [`TopicContractError::Contract`] when a topic that exists does not meet the
/// contract.
pub async fn ensure_topics(
    admin: &mut AdminClient,
    topics: &[TopicContract],
    settings: &TopicSettings,
) -> Result<TopicReport, TopicContractError> {
    settings.validate()?;
    let specs = desired_specs(topics, settings);

    for outcome in admin
        .create_topics(&specs, secs(CREATE_TIMEOUT_SECS))
        .await?
    {
        let Some(error) = outcome.error else { continue };
        if error.code == TOPIC_ALREADY_EXISTS {
            continue;
        }
        return Err(TopicContractError::Create {
            topic: outcome.name,
            code: error.code,
            name: error.name,
            message: error.message,
        });
    }

    let mut result = verify_topics(admin, topics, settings).await;
    for _ in 1..METADATA_ATTEMPTS {
        if !is_still_propagating(&result) {
            break;
        }
        sleep(METADATA_BACKOFF).await;
        result = verify_topics(admin, topics, settings).await;
    }
    result
}

/// Whether the only complaint is that a topic is not visible yet.
///
/// A create that this call made, or that a racing role made, reaches the
/// broker answering `Metadata` a moment later. Re-describing is right for
/// that, and wrong for every other kind of drift -- a wrong partition count
/// does not fix itself, and retrying it only delays the report.
fn is_still_propagating(result: &Result<TopicReport, TopicContractError>) -> bool {
    matches!(
        result,
        Err(TopicContractError::Contract { drift })
            if drift.iter().all(|d| matches!(
                d,
                TopicDrift::Missing { .. } | TopicDrift::Unreadable { .. }
            ))
    )
}
