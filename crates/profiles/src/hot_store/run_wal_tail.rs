use super::{
    PROFILES_WAL_TOPIC, ProfilesError, WalTailConfig, WalTailProfileStore, run_wal_tail_with_topic,
};

/// Consumes the default profiles WAL topic into the hot query store.
///
/// `config.wal_topic` is replaced with [`PROFILES_WAL_TOPIC`], so a caller that
/// reads the standard topic does not have to name it.
///
/// # Errors
/// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
pub async fn run_wal_tail(
    store: WalTailProfileStore,
    config: WalTailConfig,
) -> Result<(), ProfilesError> {
    run_wal_tail_with_topic(
        store,
        WalTailConfig {
            wal_topic: PROFILES_WAL_TOPIC.to_owned(),
            ..config
        },
    )
    .await
}
