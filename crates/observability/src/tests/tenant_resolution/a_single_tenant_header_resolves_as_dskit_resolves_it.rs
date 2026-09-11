use super::*;

type RowForTest<'a> = (
    &'a str,
    Option<&'a [u8]>,
    Result<TenantId, TenantRequestError>,
);

/// Every row is a header value the pinned Loki 3.5.1 image was sent on a
/// push, and the tenant or the error it answered with. `dskit` checks every
/// `|` part before it counts the distinct parts, so a malformed part wins over
/// a count, a repeated part counts once, and an empty part counts but is not
/// checked.
#[test]
pub(crate) fn a_single_tenant_header_resolves_as_dskit_resolves_it() {
    let tenant = |name: &str| Ok(TenantId::new(name).expect("a valid tenant id"));
    let unsupported = |tenant: &str, character| {
        Err(TenantRequestError::Resolve(TenantResolveError::Invalid(
            TenantIdError::UnsupportedCharacter {
                tenant: tenant.into(),
                character,
            },
        )))
    };
    let invalid = |error| {
        Err(TenantRequestError::Resolve(TenantResolveError::Invalid(
            error,
        )))
    };
    let missing = Err(TenantRequestError::Resolve(TenantResolveError::Missing));
    let multiple = Err(TenantRequestError::MultipleOrgIds);
    let too_long = "x".repeat(151);

    let cases: [RowForTest<'_>; 15] = [
        ("named", Some(b"tenant-a"), tenant("tenant-a")),
        ("absent", None, missing.clone()),
        ("empty", Some(b""), missing.clone()),
        ("separator", Some(b"a/b"), unsupported("a/b", '/')),
        (
            "too long",
            Some(too_long.as_bytes()),
            invalid(TenantIdError::TooLong),
        ),
        (
            "dot dot",
            Some(b".."),
            invalid(TenantIdError::RelativePathSegment),
        ),
        ("two tenants", Some(b"a|b"), multiple.clone()),
        (
            "an invalid second part",
            Some(b"a|b/c"),
            unsupported("b/c", '/'),
        ),
        ("an empty middle part", Some(b"a||b"), multiple.clone()),
        ("a padded part", Some(b"a | b"), unsupported("a ", ' ')),
        ("one tenant twice", Some(b"a|a"), tenant("a")),
        ("a trailing empty part", Some(b"a|"), multiple.clone()),
        ("a leading empty part", Some(b"|a"), multiple),
        // Upstream serves these as the empty tenant. Krabka cannot hold one.
        ("only a separator", Some(b"|"), missing.clone()),
        ("only separators", Some(b"||"), missing),
    ];

    for (name, value, expected) in cases {
        check!(resolve_single_tenant(value) == expected, "{name}");
    }
}
