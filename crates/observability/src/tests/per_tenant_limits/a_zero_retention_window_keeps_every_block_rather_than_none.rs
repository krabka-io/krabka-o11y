use super::*;

struct RetentionCase {
    description: &'static str,
    tenant: &'static str,
    window: Time,
}

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

    for case in [
        RetentionCase {
            description: "a tenant with a window of its own",
            tenant: "tenant-short",
            window: hours(1),
        },
        RetentionCase {
            description: "a tenant that turns retention off",
            tenant: "tenant-forever",
            window: Time::ZERO,
        },
        RetentionCase {
            description: "a tenant with no entry at all",
            tenant: "tenant-unlisted",
            window: minutes(90),
        },
    ] {
        check!(
            provider.block_retention(case.tenant) == case.window,
            "{}",
            case.description
        );
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
