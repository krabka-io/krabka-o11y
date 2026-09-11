use super::{
    Arc, BlockBuilderConfig, CancellationToken, ObjectStore, ProfilesError, run_with_config,
};

///
/// # Errors
/// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
pub async fn run(shutdown: CancellationToken) -> Result<(), ProfilesError> {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    run_with_config(
        BlockBuilderConfig::new("127.0.0.1:9092".to_string(), store),
        shutdown,
    )
    .await
}
