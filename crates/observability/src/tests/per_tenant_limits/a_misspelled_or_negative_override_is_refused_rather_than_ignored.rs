use super::*;

/// A key an operator misspells must fail the load. Ignoring it would leave
/// them believing a limit was set, and the first they would hear of it is a
/// tenant that was never capped.
///
/// A negative value must fail too. Zero is the one sentinel for "no limit",
/// and a negative cap would read as another one: every check here applies
/// only a value greater than zero.
#[test]
pub(crate) fn a_misspelled_or_negative_override_is_refused_rather_than_ignored() {
    let refused = [
        // A misspelled key in a tenant entry, in the defaults block, and at
        // the top level.
        "overrides:\n  tenant-a:\n    max_query_serie: 3\n",
        "defaults:\n  max_line_sizes: \"1KiB\"\n",
        "overrides: {}\nbogus_top_level: true\n",
        // A negative time cap and a negative size cap.
        "overrides:\n  tenant-a:\n    max_query_length: \"-1s\"\n",
        "defaults:\n  max_query_lookback: \"-1h\"\n",
        "overrides:\n  tenant-a:\n    max_line_size: \"-1B\"\n",
        // A dimensioned value with no unit is not guessed at.
        "overrides:\n  tenant-a:\n    max_query_length: 30\n",
    ];
    for yaml in refused {
        let error = OverridesProvider::from_yaml(yaml).expect_err(yaml);
        check!(matches!(error, OverridesError::Yaml(_)), "{yaml}: {error}");
    }

    // Zero is accepted on both sides, and it is what turns a cap off.
    let provider = OverridesProvider::from_yaml(
        "overrides:\n  tenant-a:\n    max_query_length: \"0s\"\n    max_line_size: \"0B\"\n",
    )
    .expect("zero turns a cap off");
    check!(
        provider
            .for_tenant(&TenantId::new("tenant-a").expect("a valid tenant id"))
            .max_query_length
            == Time::ZERO
    );
    check!(
        provider
            .for_tenant(&TenantId::new("tenant-a").expect("a valid tenant id"))
            .max_line_size
            == bytes(0)
    );
}
