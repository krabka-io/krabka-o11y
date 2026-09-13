## Summary

Describe the behavior and compatibility boundary changed by this pull request.

## Verification

- [ ] `bazel run //tools/format`
- [ ] `bazel test //...`
- [ ] `cargo test --workspace --locked`
- [ ] A relevant Docker differential suite ran, or the change does not affect an upstream compatibility claim.
- [ ] New tests exercise behavior and use `assert2` assertions.
- [ ] The change adds no `#[allow(clippy::...)]` attribute or unrequested compatibility shim.
- [ ] Public API, route inventory, compatibility, README, and coverage documents are current.
- [ ] The commit subject follows Conventional Commits.

Closes #
