//! The one `MinIO` container every signal writes to and reads from.

#[path = "../../../observability/tests/support/minio_store.rs"]
mod minio_store;

/// The `MinIO` root user, and its password.
const MINIO_USER: &str = "krabkasoak";

/// `KRABKA_MINIO_IMAGE_REF`, which the build sets to the pinned reference.
pub fn image_ref() -> Option<String> {
    std::env::var("KRABKA_MINIO_IMAGE_REF").ok()
}

/// `KRABKA_MINIO_IMAGE_ID`, which `//bazel:docker_test.sh` sets to the content
/// ID Docker gave the loaded image. A cargo run does not set it.
pub fn image_id() -> Option<String> {
    std::env::var("KRABKA_MINIO_IMAGE_ID").ok()
}

/// Starts `MinIO` with the bucket already made, and an S3 store on it.
pub async fn start() -> minio_store::Minio {
    minio_store::start_minio(MINIO_USER).await
}
