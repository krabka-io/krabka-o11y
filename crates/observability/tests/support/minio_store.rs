//! A `MinIO` container with its bucket already made, and an S3 `ObjectStore`
//! over it.
//!
//! The `scale_object_store` suite here and the operating-envelope soak in
//! `//crates/integration` reach this file with `#[path]`.

use std::{sync::Arc, time::Duration};

use object_store::{ObjectStore, aws::AmazonS3Builder};
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt,
    core::{ContainerPort, WaitFor},
    runners::AsyncRunner as _,
};

/// The bucket the container is started with.
const BUCKET: &str = "krabka";
const MINIO_API_PORT: u16 = 9000;

/// Starting a container and waiting for its API is not the thing under test, so
/// it gets a bound of its own rather than the suite's.
const CONTAINER_START_TIMEOUT: Duration = Duration::from_mins(2);

/// A running `MinIO`, and an `ObjectStore` pointed at its bucket.
///
/// The container is held alongside the store because testcontainers stops it
/// when the handle drops, and a store outliving its container is a test that
/// fails for a reason that has nothing to do with the code.
pub struct Minio {
    _container: ContainerAsync<GenericImage>,
    pub store: Arc<dyn ObjectStore>,
}

/// The repository and tag of the `MinIO` image the build loaded into the
/// daemon.
///
/// # Panics
///
/// Panics when `KRABKA_MINIO_IMAGE_REF` is unset or holds no tag. The build
/// sets it for `bazel test --config=scale`; under cargo, set it to the
/// `minio` reference in `//bazel/images/images.bzl` after loading that image.
fn minio_image() -> (String, String) {
    let reference = std::env::var("KRABKA_MINIO_IMAGE_REF").expect(
        "KRABKA_MINIO_IMAGE_REF is unset. This suite runs under `bazel test --config=scale`, \
         which loads the pinned image and sets this. To run it under cargo, set it to the \
         `minio` reference in //bazel/images/images.bzl.",
    );
    let (repository, tag) = reference
        .rsplit_once(':')
        .expect("the MinIO image reference carries a tag");
    (repository.to_string(), tag.to_string())
}

/// Starts `MinIO` with the bucket already made, signing in as `root_user`
/// with the same string as its password.
///
/// # Panics
///
/// Panics when the image reference is missing, or when the container does not
/// start inside its timeout.
pub async fn start_minio(root_user: &str) -> Minio {
    // No default. //bazel/defs.bzl sets this from //bazel/images/images.bzl,
    // the same map that decides what `docker load` tags. A default here would
    // be a second copy of that decision, and when the two disagree
    // testcontainers pulls the image over the network instead of using the
    // pinned bytes. The whole reference comes from the build, repository
    // included: the image is assembled from a package lock rather than pulled
    // from a registry, so no registry name belongs in this file.
    let (repository, tag) = minio_image();

    // The bucket is a directory under the data root, made before MinIO reads
    // it. MinIO has no "create this bucket at startup" switch, `object_store`
    // has no bucket-creation call, and creating one over the API needs a
    // SigV4-signed `PUT /<bucket>` that nothing in this dependency set can
    // build. Overriding the entrypoint is the one step that needs no extra
    // tool in the image and no extra crate in the manifest.
    let container = tokio::time::timeout(
        CONTAINER_START_TIMEOUT,
        GenericImage::new(repository, tag)
            .with_exposed_port(ContainerPort::Tcp(MINIO_API_PORT))
            // MinIO writes its whole banner to stderr, the `API:` line
            // included. Waiting on stdout waits for a stream that stays empty,
            // and testcontainers reports that as `WaitContainer(StartupTimeout)`
            // -- which reads like a container that failed to start, while the
            // container is up and serving.
            .with_wait_for(WaitFor::message_on_stderr("API:"))
            .with_entrypoint("/bin/sh")
            .with_env_var("MINIO_ROOT_USER", root_user)
            .with_env_var("MINIO_ROOT_PASSWORD", root_user)
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
        .with_access_key_id(root_user)
        .with_secret_access_key(root_user)
        .with_region("us-east-1")
        // MinIO speaks S3 over plain HTTP here, and the default rejects that.
        .with_allow_http(true)
        // Path style, because `http://127.0.0.1:port/krabka/...` has no
        // hostname to put a bucket in front of.
        .with_virtual_hosted_style_request(false)
        .build()
        .expect("the S3 store is configured");

    Minio {
        _container: container,
        store: Arc::new(store),
    }
}
