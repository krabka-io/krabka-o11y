use super::*;

/// Every field of the limit set has to survive a write and a read. A field
/// whose serde attribute names the wrong unit adapter, or whose key differs
/// between the full struct and the sparse override struct, would load as a
/// default and the operator would never see it refused.
///
/// The round trip goes through the sparse override struct, which is the one
/// an operator's file is read into, so the two structs are checked against
/// each other rather than each against itself.
#[test]
pub(crate) fn the_overrides_file_round_trips_through_the_limit_set() {
    let limits = Limits {
        max_line_size: bytes(4096),
        max_label_names_per_series: 7,
        max_label_name_length: bytes(64),
        max_label_value_length: bytes(128),
        reject_old_samples_max_age: days(3),
        creation_grace_period: minutes(4),
        max_ingest_body: bytes(2048),
        max_query_length: hours(5),
        max_query_lookback: days(6),
        max_entries_limit_per_query: 11,
        max_query_series: 12,
        max_query_read: bytes(8192),
        max_query_string_bytes: bytes(256),
        max_query_range: secs(13),
        retention_period: days(14),
    };

    // `Limits` serialises to exactly the keys the override struct reads, so
    // one tenant's whole set can be written back out as its override.
    let body = serde_yaml::to_string(&limits).expect("the limit set serialises");
    let yaml = String::from("overrides:\n  tenant-a:\n")
        + &body
            .lines()
            .map(|line| String::from("    ") + line)
            .collect::<Vec<_>>()
            .join("\n");

    let provider = OverridesProvider::from_yaml(&yaml).expect("the written file parses");
    check!(*provider.for_tenant(&TenantId::new("tenant-a").expect("a valid tenant id")) == limits);

    // And the `defaults` block reads the same keys.
    let yaml = yaml.replace("overrides:\n  tenant-a:\n", "defaults:\n");
    let provider = OverridesProvider::from_yaml(&yaml).expect("the defaults block parses");
    check!(*provider.defaults() == limits);
}
