use super::*;

type RowForTest = (
    &'static str,
    Option<&'static [u8]>,
    Result<Vec<TenantId>, TenantRequestError>,
);

/// Every row is a header value the pinned Loki 3.5.1 image, with
/// `querier.multi_tenant_queries_enabled` on, was sent on a range query. A
/// malformed part refuses the whole query, and an empty part names no tenant.
#[test]
pub(crate) fn a_federated_header_resolves_every_part_sorted_and_without_repeats() {
    let tenants = |names: &[&str]| {
        Ok(names
            .iter()
            .map(|name| TenantId::new(*name).expect("a valid tenant id"))
            .collect::<Vec<_>>())
    };
    let unsupported = |tenant: &str, character| {
        Err(TenantRequestError::Resolve(TenantResolveError::Invalid(
            TenantIdError::UnsupportedCharacter {
                tenant: tenant.into(),
                character,
            },
        )))
    };
    let missing = Err(TenantRequestError::Resolve(TenantResolveError::Missing));

    let cases: [RowForTest; 11] = [
        ("one tenant", Some(b"tenant-a"), tenants(&["tenant-a"])),
        ("absent", None, missing.clone()),
        ("empty", Some(b""), missing.clone()),
        ("two tenants", Some(b"b|a"), tenants(&["a", "b"])),
        ("a repeat", Some(b"b|a|b"), tenants(&["a", "b"])),
        ("an empty middle part", Some(b"a||b"), tenants(&["a", "b"])),
        ("a trailing empty part", Some(b"a|"), tenants(&["a"])),
        ("an invalid part", Some(b"a|b/c"), unsupported("b/c", '/')),
        ("a padded part", Some(b"a | b"), unsupported("a ", ' ')),
        (
            "a dot dot part",
            Some(b"a|.."),
            Err(TenantRequestError::Resolve(TenantResolveError::Invalid(
                TenantIdError::RelativePathSegment,
            ))),
        ),
        ("only a separator", Some(b"|"), missing),
    ];

    for (name, value, expected) in cases {
        check!(resolve_federated_tenants(value) == expected, "{name}");
    }
}
