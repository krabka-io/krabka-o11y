use krabka_observability::persisted_format::PersistedFormatError;

use super::{
    ConsumerRecord, Time, WalHeadConsumerError, WalHeadConsumerPoll, WalHeadConsumerRecord,
    checked_replay_records,
};

pub(crate) async fn poll_checked_replay_records<C, E>(
    consumer: &mut C,
    topic: &str,
    timeout: Time,
    observe_records: impl FnOnce(&[ConsumerRecord]),
    map_poll_error: impl FnOnce(WalHeadConsumerError) -> E,
    map_format_error: impl FnOnce(PersistedFormatError) -> E,
) -> Result<Vec<WalHeadConsumerRecord>, E>
where
    C: WalHeadConsumerPoll + ?Sized,
{
    let records = consumer.poll(timeout).await.map_err(map_poll_error)?;
    observe_records(&records);
    checked_replay_records(records, topic).map_err(map_format_error)
}
