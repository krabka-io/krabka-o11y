use super::*;

/// An override is sparse by design: the operator writes the one limit they
/// want changed, and the tenant keeps every other value from the defaults.
/// A merge that reset the unnamed fields would silently turn `Loki`'s own
/// defaults off for that tenant, which is the failure this checks for.
///
/// The `defaults` block is checked in the same test, because it merges
/// through the same code and a tenant with no entry of its own must see it.
#[test]
pub(crate) fn a_tenant_override_changes_only_the_limits_it_names() {
    const YAML: &str = r#"
defaults:
  max_line_size: "1KiB"
overrides:
  tenant-capped:
    max_query_series: 2
    max_line_size: "16B"
  tenant-open:
    max_query_length: "0s"
"#;

    let provider = OverridesProvider::from_yaml(YAML).expect("the overrides file parses");

    // The named tenant keeps every limit it did not name.
    let capped = provider.for_tenant(&TenantId::new("tenant-capped").expect("a valid tenant id"));
    check!(capped.max_query_series == 2);
    check!(capped.max_line_size == bytes(16));
    check!(
        capped.max_label_names_per_series == Limits::default().max_label_names_per_series,
        "an unnamed limit keeps Loki's default"
    );
    check!(
        capped.max_query_length == Limits::default().max_query_length,
        "and so does an unnamed time cap"
    );

    // A tenant with an entry that names something else still gets the
    // `defaults` block, not the built-in default.
    let open = provider.for_tenant(&TenantId::new("tenant-open").expect("a valid tenant id"));
    check!(open.max_query_length == Time::ZERO, "zero turns a cap off");
    check!(
        open.max_line_size == bytes(1024),
        "the defaults block applies"
    );

    // A tenant with no entry at all gets the `defaults` block too.
    let unlisted =
        provider.for_tenant(&TenantId::new("tenant-unlisted").expect("a valid tenant id"));
    check!(unlisted.max_line_size == bytes(1024));
    check!(*unlisted == *provider.defaults());
    check!(
        !provider
            .has_tenant_override(&TenantId::new("tenant-unlisted").expect("a valid tenant id"))
    );
    check!(
        provider.has_tenant_override(&TenantId::new("tenant-capped").expect("a valid tenant id"))
    );
}
