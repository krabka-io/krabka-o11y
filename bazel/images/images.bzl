"""The reference each differential-suite image is loaded into the daemon under.

//MODULE.bazel pins the bytes by digest; this names them. A suite asks
testcontainers for an image by reference, so the reference it asks for has to
be the one `docker load` just created -- ask for anything else and
testcontainers finds nothing locally and pulls it over the network mid-test,
which is the whole thing the digest pin exists to prevent.

Keeping that in one place is the point of this file. It was previously written
twice: here, and again as a default in each suite's source. The two drifted --
`grafana_e2e` asked for `grafana:11.5.2` and `prometheus:v3.1.0` while the
build loaded different versions, so that suite pulled both images from the
network on every run and compared against whatever it got. //bazel/defs.bzl now
hands each suite the reference from this map, and no suite carries a default.

The tag half of a reference is a local label for bytes a digest already fixed;
`docker load` re-creates it from the pinned tarball before every run.
"""

ORACLES = {
    "loki": struct(
        binary = "/usr/bin/loki",
        image = "mirror.gcr.io/grafana/loki:3.7.7",
        revision = "7a40404f32b3e6464c9cfc6cc7dd75a40f3931da",
    ),
    "mimir": struct(
        binary = "/bin/mimir",
        image = "mirror.gcr.io/grafana/mimir:3.2.1",
        revision = "e49585d43c6e852225e114bd1ddd98da58a4c060",
    ),
    "pyroscope": struct(
        binary = "/usr/bin/pyroscope",
        image = "mirror.gcr.io/grafana/pyroscope:2.3.1",
        revision = "7aeaa0ff91e83538b3ff0d09bfefb168bddc022d",
    ),
    "tempo": struct(
        binary = "/tempo",
        image = "mirror.gcr.io/grafana/tempo:3.0.3",
        revision = "1900ed7bb5cad1a3edc285783d7d4ac4278337dc",
    ),
}

CLIENTS = {
    "alloy": struct(
        binary = "/bin/alloy",
        image = "mirror.gcr.io/grafana/alloy:v1.19.2",
        revision = "becfd489a7bb459c0496893b555fb87a003296b1",
        version = "1.19.2",
    ),
    "grafana": struct(
        binary = "/usr/share/grafana/bin/grafana",
        image = "mirror.gcr.io/grafana/grafana:13.2.2",
        revision = "",
        version = "13.2.2",
    ),
    "prometheus": struct(
        binary = "/bin/prometheus",
        image = "mirror.gcr.io/prom/prometheus:v3.14.0",
        revision = "d7598b7141418fa35be2b5ec5d0fefb634199610",
        version = "3.14.0",
    ),
}

IMAGES = {
    "alloy": CLIENTS["alloy"].image,
    "grafana": CLIENTS["grafana"].image,
    "loki": ORACLES["loki"].image,
    "mimir": ORACLES["mimir"].image,
    "minio": "mirror.gcr.io/minio/minio:RELEASE.2025-04-22T22-12-26Z",
    "prometheus": CLIENTS["prometheus"].image,
    "pyroscope": ORACLES["pyroscope"].image,
    "tempo": ORACLES["tempo"].image,
}

def image_tag_env(name):
    """The environment variable a suite reads image `name`'s tag from."""
    return "KRABKA_" + name.upper() + "_IMAGE_TAG"
