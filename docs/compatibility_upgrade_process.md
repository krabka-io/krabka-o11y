# Upstream Compatibility Upgrades

Use this process when Krabka changes a Mimir, Loki, Tempo, or Pyroscope oracle.
The pull request is the review record for the change.
[`docs/api/upstream_surfaces.json`](api/upstream_surfaces.json) is the source of truth for each oracle baseline.
It records the tag, image, platform, digest, and upstream Git revision.

## Select the release

1. Select a release tag from the official upstream repository.
2. Resolve the source revision for that tag.
3. Resolve the release image to its `linux/amd64` OCI manifest digest.
4. Record the tag, image, platform, digest, and revision in the upstream surface manifest.
5. Check that the image reports the selected version and revision.

Confirm that the tag, revision, image name, platform, digest, and reported version identify one release.
Do not use a mutable tag as release evidence.

## Update the contract

1. Update the tagged source and surface list in the upstream surface manifest.
2. Classify each new surface as required, role-equivalent, or excluded.
3. Set the image and revision in [`bazel/images/images.bzl`](../bazel/images/images.bzl) to the manifest values.
4. Set the platform, image repository, and digest in [`MODULE.bazel`](../MODULE.bazel) to the manifest values.
5. Update the [compatibility matrix](api_compatibility.md) when evidence or scope changes.

`tools/upstream-surface.py --check` fails if the manifest has an unclassified surface.
It also fails if `images.bzl` or `MODULE.bazel` differs from the manifest baseline.
Give the product boundary or the replacement workflow for each exclusion.

## Verify the change

Run these checks before review:

```sh
tools/upstream-surface.py --self-test
tools/upstream-surface.py --check
bazel test --config=docker //bazel/images:loki_version_test //bazel/images:mimir_version_test //bazel/images:tempo_version_test //bazel/images:pyroscope_version_test
bazel test --config=docker //crates/metrics-service:diff_mimir_docker_test
bazel test --config=docker //crates/observability:loki_differential_docker_test
bazel test --config=docker //crates/traces:tempo_differential_docker_test
bazel test --config=docker //crates/profiles:pyroscope_differential_docker_test
```

Run only the differential suite for the changed product.
Run all four suites when the shared manifest or compatibility rules change.
The image smoke test fails when a changed image reports the wrong version.
The CI Docker selector runs the oracle version tests with the selected container suites.

## Supply review evidence

Include this evidence in the pull request:

- The official release tag and source revision.
- The image name, platform, and OCI manifest digest.
- The image-reported version and revision output.
- The surface manifest diff and all classification reasons.
- Links to the completed differential jobs.
- The compatibility-matrix diff, or a statement that no claim changed.

A reviewer checks that the six items identify the same upstream release and test result.
Merge only after the surface check and the applicable differential jobs pass.

## Milestones 8 and 9

Milestone 8 completed the planned Loki, Tempo, and Pyroscope parity work.
Milestone 9 completed the Krabka route inventory and the compatibility documentation.
Both milestones are closed, and this process does not add scope to them.
Milestone 10 adds immutable oracles and classifies their implemented surfaces against tagged upstream sources.
Later parity work belongs to milestones 11 through 14.
