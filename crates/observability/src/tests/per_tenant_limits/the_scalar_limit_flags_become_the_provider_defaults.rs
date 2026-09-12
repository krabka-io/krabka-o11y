use super::*;

/// The scalar CLI flags are the process defaults the overrides file merges
/// over. An operator who sets `--max-query-range` and writes no file must
/// still get that value for every tenant, and a flag they left unset must
/// leave `Loki`'s own default in place.
#[test]
pub(crate) fn the_scalar_limit_flags_become_the_provider_defaults() {
    // Nothing set: every limit is Loki's own default.
    check!(limits_for_config(&ServiceConfig::default()) == Limits::default());

    let config = ServiceConfig {
        max_query_range: Some(secs(30)),
        max_query_series: Some(11),
        max_query_read: Some(bytes(4096)),
        max_query_string_bytes: Some(bytes(64)),
        max_ingest_body: Some(bytes(2048)),
        retention_period: Some(days(30)),
        reject_old_samples_max_age: days(3),
        creation_grace_period: minutes(4),
        ..ServiceConfig::default()
    };
    let limits = limits_for_config(&config);

    check!(
        limits
            == Limits {
                max_query_range: secs(30),
                max_query_series: 11,
                max_query_read: bytes(4096),
                max_query_string_bytes: bytes(64),
                max_ingest_body: bytes(2048),
                retention_period: days(30),
                reject_old_samples_max_age: days(3),
                creation_grace_period: minutes(4),
                ..Limits::default()
            }
    );

    // The file's `defaults` block wins over the flag, because it is the one
    // an operator can change without restarting the process.
    let provider = OverridesProvider::from_yaml_over(
        "defaults:\n  max_query_series: 5\n",
        &limits_for_config(&config),
    )
    .expect("the overrides file parses");
    check!(provider.defaults().max_query_series == 5);
    check!(
        provider.defaults().max_query_read == bytes(4096),
        "a flag the file does not name still applies"
    );
}
