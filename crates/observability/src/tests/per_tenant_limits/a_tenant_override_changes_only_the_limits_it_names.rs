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
  discover_log_levels: false
  log_level_fields: [priority, SeverityText]
  log_level_from_json_max_depth: 4
overrides:
  tenant-capped:
    max_query_series: 2
    max_line_size: "16B"
    discover_log_levels: true
    log_level_fields: []
    log_level_from_json_max_depth: 0
  tenant-open:
    max_query_length: "0s"
    log_level_from_json_max_depth: -2
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

    check!(
        *capped
            == Limits {
                max_query_series: 2,
                max_line_size: bytes(16),
                discover_log_levels: true,
                log_level_fields: Vec::new(),
                log_level_from_json_max_depth: 0,
                ..Limits::default()
            }
    );

    // A tenant with an entry that names something else still gets the
    // `defaults` block, not the built-in default.
    let open = provider.for_tenant(&TenantId::new("tenant-open").expect("a valid tenant id"));
    check!(open.max_query_length == Time::ZERO, "zero turns a cap off");
    check!(
        open.max_line_size == bytes(1024),
        "the defaults block applies"
    );

    check!(
        *open
            == Limits {
                max_line_size: bytes(1024),
                max_query_length: Time::ZERO,
                discover_log_levels: false,
                log_level_fields: vec!["priority".into(), "SeverityText".into()],
                log_level_from_json_max_depth: -2,
                ..Limits::default()
            }
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
