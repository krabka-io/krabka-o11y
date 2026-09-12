use super::*;

/// The sentinel reading, pinned in both directions.
///
/// `Loki` ships `retention_period: 0s`, and zero there means "no retention".
/// Read the other way round it means "delete everything", and the first sweep
/// of a fresh deployment would then delete every block of every tenant that
/// configured nothing. That is not a bug a later fix can undo.
#[test]
pub(crate) fn a_zero_retention_window_keeps_every_block_rather_than_none() {
    // `Loki`'s own default, which is what an operator who writes no file gets.
    check!(Limits::default().retention_period == Time::ZERO);
    check!(!OverridesProvider::new(Limits::default()).expires_any_blocks());

    let provider = OverridesProvider::from_yaml(
        "defaults:\n  retention_period: \"90m\"\noverrides:\n  tenant-short:\n    \
         retention_period: \"1h\"\n  tenant-forever:\n    retention_period: \"0s\"\n",
    )
    .expect("the overrides file parses");

    for (name, tenant, window) in [
        (
            "a tenant with a window of its own",
            "tenant-short",
            hours(1),
        ),
        (
            "a tenant that turns retention off",
            "tenant-forever",
            Time::ZERO,
        ),
        (
            "a tenant with no entry at all",
            "tenant-unlisted",
            minutes(90),
        ),
    ] {
        check!(provider.block_retention(tenant) == window, "{name}");
    }

    // The sweep costs a pass over the object store, so a deployment where no
    // window is set anywhere must not pay for one.
    check!(provider.expires_any_blocks());
    check!(
        !OverridesProvider::from_yaml("overrides:\n  tenant-a:\n    retention_period: \"0s\"\n")
            .expect("the overrides file parses")
            .expires_any_blocks()
    );
}
