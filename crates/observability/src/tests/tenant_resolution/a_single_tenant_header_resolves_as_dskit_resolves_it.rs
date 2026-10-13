use super::*;

struct RowForTest<'a> {
    name: &'a str,
    value: Option<&'a [u8]>,
    expected: Result<TenantId, TenantRequestError>,
}

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
        RowForTest {
            name: "named",
            value: Some(b"tenant-a"),
            expected: tenant("tenant-a"),
        },
        RowForTest {
            name: "absent",
            value: None,
            expected: missing.clone(),
        },
        RowForTest {
            name: "empty",
            value: Some(b""),
            expected: missing.clone(),
        },
        RowForTest {
            name: "separator",
            value: Some(b"a/b"),
            expected: unsupported("a/b", '/'),
        },
        RowForTest {
            name: "dot dot",
            value: Some(b".."),
            expected: invalid(TenantIdError::RelativePathSegment),
        },
        RowForTest {
            name: "two tenants",
            value: Some(b"a|b"),
            expected: multiple.clone(),
        },
        RowForTest {
            name: "an invalid second part",
            value: Some(b"a|b/c"),
            expected: unsupported("b/c", '/'),
        },
        RowForTest {
            name: "an empty middle part",
            value: Some(b"a||b"),
            expected: multiple.clone(),
        },
        RowForTest {
            name: "a padded part",
            value: Some(b"a | b"),
            expected: unsupported("a ", ' '),
        },
        RowForTest {
            name: "one tenant twice",
            value: Some(b"a|a"),
            expected: tenant("a"),
        },
        RowForTest {
            name: "a trailing empty part",
            value: Some(b"a|"),
            expected: multiple.clone(),
        },
        RowForTest {
            name: "a leading empty part",
            value: Some(b"|a"),
            expected: multiple,
        },
        // Upstream serves these as the empty tenant. Krabka cannot hold one.
        RowForTest {
            name: "only a separator",
            value: Some(b"|"),
            expected: missing.clone(),
        },
        RowForTest {
            name: "only separators",
            value: Some(b"||"),
            expected: missing,
        },
        RowForTest {
            name: "too long",
            value: Some(too_long.as_bytes()),
            expected: invalid(TenantIdError::TooLong),
        },
    ];

    for row in cases {
        check!(
            resolve_single_tenant(row.value) == row.expected,
            "{}",
            row.name
        );
    }
}
