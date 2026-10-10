//! The digest-pinned Prometheus image that every Prometheus oracle suite
//! starts.

use testcontainers::GenericImage;

/// The Prometheus image `bazel test --config=docker` loaded, by the tag it set.
///
/// There is no default. //bazel/defs.bzl sets the tag from
/// //bazel/images/images.bzl, the same map that decides what `docker load`
/// tags. A default here would be a second copy of that decision, and when the
/// two disagreed testcontainers pulled the image over the network and the suite
/// compared against whatever it got rather than against the pinned bytes.
///
/// # Panics
/// Panics when `KRABKA_PROMETHEUS_IMAGE_TAG` is unset.
pub(crate) fn pinned_prometheus_image() -> GenericImage {
    let tag = std::env::var("KRABKA_PROMETHEUS_IMAGE_TAG").expect(
        "KRABKA_PROMETHEUS_IMAGE_TAG is unset. These suites run under `bazel test --config=docker`, \
         which loads the digest-pinned image and sets this. To run one under \
         cargo, set it to that image's tag in //bazel/images/images.bzl.",
    );
    GenericImage::new("mirror.gcr.io/prom/prometheus".to_string(), tag)
}
