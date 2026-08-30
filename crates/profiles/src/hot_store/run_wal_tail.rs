use super::{
    PROFILES_WAL_TOPIC, ProfilesError, Time, WalTailProfileStore, run_wal_tail_with_topic,
};

///
/// # Errors
/// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
pub async fn run_wal_tail(
    store: WalTailProfileStore,
    bootstrap: String,
    group_id: String,
    poll_timeout: Time,
    client_dispatch_queue_capacity: krabka_client_core::ConnectionDispatchQueueCapacity,
    client_frame_max: krabka_client_core::ClientFrameMax,
) -> Result<(), ProfilesError> {
    run_wal_tail_with_topic(
        store,
        bootstrap,
        group_id,
        PROFILES_WAL_TOPIC.to_owned(),
        poll_timeout,
        client_dispatch_queue_capacity,
        client_frame_max,
    )
    .await
}
