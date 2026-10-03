//! The one `MinIO` container every signal writes to and reads from.

use std::{sync::Arc, time::Duration};

use object_store::{ObjectStore, aws::AmazonS3Builder};
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt,
    core::{ContainerPort, WaitFor},
    runners::AsyncRunner as _,
};

const BUCKET: &str = "krabka";
const MINIO_USER: &str = "krabkasoak";
const MINIO_PASSWORD: &str = "krabkasoak";
const MINIO_API_PORT: u16 = 9000;
const CONTAINER_START_TIMEOUT: Duration = Duration::from_mins(2);

/// The image reference the build loaded, split into repository and tag.
pub fn image() -> (String, String) {
    let reference = image_ref().expect(
        "KRABKA_MINIO_IMAGE_REF is unset. This suite runs under `bazel test --config=scale`, \
         which loads the pinned image and sets this. To run it under cargo, set it to the \
         `minio` reference in //bazel/images/images.bzl.",
    );
    let (repository, tag) = reference
        .rsplit_once(':')
        .expect("the MinIO image reference carries a tag");
    (repository.to_string(), tag.to_string())
}

/// `KRABKA_MINIO_IMAGE_REF`, which the build sets to the pinned reference.
pub fn image_ref() -> Option<String> {
    std::env::var("KRABKA_MINIO_IMAGE_REF").ok()
}

/// `KRABKA_MINIO_IMAGE_ID`, which `//bazel:docker_test.sh` sets to the content
/// ID Docker gave the loaded image. A cargo run does not set it.
pub fn image_id() -> Option<String> {
    std::env::var("KRABKA_MINIO_IMAGE_ID").ok()
}

/// Starts `MinIO` with the bucket already made, and returns an S3 store on it.
pub async fn start() -> (ContainerAsync<GenericImage>, Arc<dyn ObjectStore>) {
    let (repository, tag) = image();

    let container = tokio::time::timeout(
        CONTAINER_START_TIMEOUT,
        GenericImage::new(repository, tag)
            .with_exposed_port(ContainerPort::Tcp(MINIO_API_PORT))
            // MinIO writes its banner, the `API:` line included, to stderr.
            .with_wait_for(WaitFor::message_on_stderr("API:"))
            // The bucket is a directory made before MinIO reads the data root.
            .with_entrypoint("/bin/sh")
            .with_env_var("MINIO_ROOT_USER", MINIO_USER)
            .with_env_var("MINIO_ROOT_PASSWORD", MINIO_PASSWORD)
            .with_cmd([
                "-c",
                &format!("mkdir -p /data/{BUCKET} && exec /usr/bin/minio server /data"),
            ])
            .start(),
    )
    .await
    .expect("MinIO started inside its timeout")
    .expect("MinIO started");

    let port = container
        .get_host_port_ipv4(MINIO_API_PORT)
        .await
        .expect("the API port is mapped");

    let store = AmazonS3Builder::new()
        .with_endpoint(format!("http://127.0.0.1:{port}"))
        .with_bucket_name(BUCKET)
        .with_access_key_id(MINIO_USER)
        .with_secret_access_key(MINIO_PASSWORD)
        .with_region("us-east-1")
        .with_allow_http(true)
        .with_virtual_hosted_style_request(false)
        .build()
        .expect("the S3 store is configured");

    (container, Arc::new(store))
}
