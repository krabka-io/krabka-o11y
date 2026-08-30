use super::*;

///
/// # Errors
/// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
pub async fn run(debuginfod_urls: Vec<String>) -> Result<(), crate::ProfilesError> {
    run_with_config(debuginfod_urls, DebuginfodConfig::default()).await
}
