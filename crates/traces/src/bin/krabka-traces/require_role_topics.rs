use krabka_observability::topic_contract::{TRACES_TOPICS, TopicContractError, require_topics};

use super::{Cli, Target};

/// Refuses to let this role start on topics that do not meet the contract.
///
/// A WAL topic's partition count is the write-path shard count, and the traces
/// WAL is keyed by trace id: change the count and one trace's spans split
/// across two partitions with no order between them, which nothing on the wire
/// reports. The check therefore runs before any producer or consumer is built.
///
/// Three of these roles reach no broker at all. The query-frontend fans out to
/// queriers over HTTP, the compactor reads and writes only the object store,
/// and a querier tails the WAL only with `--querier-live-store`. Asking those
/// to validate a topic would make a broker they do not use a condition of
/// their starting, which is a fault they do not have.
///
/// # Errors
/// Returns [`TopicContractError`] when no bootstrap address answers, or when a
/// topic is absent.
pub(crate) async fn require_role_topics(cli: &Cli) -> Result<(), TopicContractError> {
    if !touches_the_wal(cli) {
        return Ok(());
    }
    require_topics(&cli.bootstrap, &TRACES_TOPICS).await?;
    Ok(())
}

/// Whether this role opens a WAL client. The match is exhaustive so a new role
/// has to answer the question rather than inherit an answer.
fn touches_the_wal(cli: &Cli) -> bool {
    match cli.target {
        Target::Distributor
        | Target::BlockBuilder
        | Target::LiveStore
        | Target::MetricsGenerator => true,
        // Only the embedded live tier consumes; a querier reading a remote
        // live-store, or none at all, speaks to no broker.
        Target::Querier => cli.querier_live_store,
        Target::QueryFrontend | Target::Compactor => false,
    }
}
