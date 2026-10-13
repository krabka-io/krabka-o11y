use super::*;

struct RowForTest {
    name: &'static str,
    value: Option<&'static [u8]>,
    expected: Result<Vec<TenantId>, TenantRequestError>,
}

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
        RowForTest {
            name: "one tenant",
            value: Some(b"tenant-a"),
            expected: tenants(&["tenant-a"]),
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
            name: "two tenants",
            value: Some(b"b|a"),
            expected: tenants(&["a", "b"]),
        },
        RowForTest {
            name: "a repeat",
            value: Some(b"b|a|b"),
            expected: tenants(&["a", "b"]),
        },
        RowForTest {
            name: "an empty middle part",
            value: Some(b"a||b"),
            expected: tenants(&["a", "b"]),
        },
        RowForTest {
            name: "a trailing empty part",
            value: Some(b"a|"),
            expected: tenants(&["a"]),
        },
        RowForTest {
            name: "an invalid part",
            value: Some(b"a|b/c"),
            expected: unsupported("b/c", '/'),
        },
        RowForTest {
            name: "a padded part",
            value: Some(b"a | b"),
            expected: unsupported("a ", ' '),
        },
        RowForTest {
            name: "a dot dot part",
            value: Some(b"a|.."),
            expected: Err(TenantRequestError::Resolve(TenantResolveError::Invalid(
                TenantIdError::RelativePathSegment,
            ))),
        },
        RowForTest {
            name: "only a separator",
            value: Some(b"|"),
            expected: missing,
        },
    ];

    for case in cases {
        check!(
            resolve_federated_tenants(case.value) == case.expected,
            "{}",
            case.name
        );
    }
}
