# API Inventory

[`routes.json`](routes.json) lists every HTTP method and path registered by the four signal implementations. It also records every Profiles protobuf file hash, service, method, message, field, enum, and value.

The generator reads production router declarations and the local Pyroscope and OTLP protobuf definitions.

Regenerate it with:

```bash
tools/route-inventory.py > docs/api/routes.json
```

CI runs `tools/route-inventory.py --check`, so a route change must update this file.

## Upstream surface inventory

[`upstream_surfaces.json`](upstream_surfaces.json) records the public API contract
for each pinned upstream tag.
It classifies each HTTP and protocol surface as `required`, `role-equivalent`, or `excluded`.
Each classification links to the tagged upstream source that defines the surface.
This manifest is the source of truth for the oracle baseline.
It records each image, Git revision, `linux/amd64` platform, and image digest.

The check compares that baseline with
[`images.bzl`](../../bazel/images/images.bzl) and [`MODULE.bazel`](../../MODULE.bazel).
It fails if an image, revision, platform, or digest changes in only one file.

Run these checks after an upstream tag or surface changes:

```bash
tools/upstream-surface.py --self-test
tools/upstream-surface.py --check
```

Add a new upstream surface to `surfaces` first.
The check fails until one classification includes that surface.
