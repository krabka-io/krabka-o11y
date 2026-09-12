"""The reference each differential-suite image is loaded into the daemon under.

//MODULE.bazel pins the bytes by digest; this names them. A suite asks
testcontainers for an image by reference, so the reference it asks for has to
be the one `docker load` just created -- ask for anything else and
testcontainers finds nothing locally and pulls it over the network mid-test,
which is the whole thing the digest pin exists to prevent.

Keeping that in one place is the point of this file. It was previously written
twice: here, and again as a default in each suite's source. The two drifted --
`grafana_e2e` asked for `grafana:11.5.2` and `prometheus:v3.1.0` while the
build loaded `11.6.1` and `v3.8.0`, so that suite pulled both images from the
network on every run and compared against whatever it got. //bazel/defs.bzl now
hands each suite the reference from this map, and no suite carries a default.

The tag half of a reference is a local label for bytes a digest already fixed,
so `latest` here is not the moving tag it looks like: `docker load` re-creates
it from the pinned tarball before every run. It is written this way for the two
images whose pinned digest carries no version to name it by.
"""

IMAGES = {
    "grafana": "mirror.gcr.io/grafana/grafana:11.6.1",
    "loki": "mirror.gcr.io/grafana/loki:3.5.1",
    "mimir": "mirror.gcr.io/grafana/mimir:2.16.1",
    "minio": "mirror.gcr.io/minio/minio:RELEASE.2025-04-22T22-12-26Z",
    "prometheus": "mirror.gcr.io/prom/prometheus:v3.8.0",
    "pyroscope": "mirror.gcr.io/grafana/pyroscope:latest",
    "tempo": "mirror.gcr.io/grafana/tempo:latest",
}

def image_tag_env(name):
    """The environment variable a suite reads image `name`'s tag from."""
    return "KRABKA_" + name.upper() + "_IMAGE_TAG"
