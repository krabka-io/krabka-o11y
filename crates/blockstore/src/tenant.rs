//! Tenant identity, shared by every Krabka signal.
//!
//! `krabka-blockstore` sits below every other crate in the workspace, so the
//! tenant type lives here and the metrics, logs, traces and profiles paths all
//! speak it. [`TenantId`] holds the charset rules that Grafana Mimir's
//! `tenant.ValidTenantID` sets, and [`ANONYMOUS_TENANT`] names the tenant that
//! Grafana Tempo and Grafana Pyroscope fall back to in single-tenant mode.

use std::str::FromStr;

use derive_more::Display;
use serde::{Deserialize, Deserializer, Serialize, de::Error as DeError};
use thiserror::Error;

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::{
        ANONYMOUS_TENANT, MAX_TENANT_ID_LEN, TenantId, TenantIdError, TenantPolicy,
        TenantResolveError,
    };

    struct TenantResolutionCase<'a> {
        name: &'a str,
        value: Option<&'a [u8]>,
        required: Result<TenantId, TenantResolveError>,
        fallback: Result<TenantId, TenantResolveError>,
    }

    struct ValidTenantCase<'a> {
        description: &'a str,
        id: &'a str,
    }

    struct InvalidTenantCase<'a> {
        description: &'a str,
        id: &'a str,
        expected: String,
    }

    #[test]
    fn every_valid_shape_is_accepted() {
        let valid = [
            ValidTenantCase {
                description: "plain",
                id: "tenant-a",
            },
            ValidTenantCase {
                description: "underscore",
                id: "team_42",
            },
            ValidTenantCase {
                description: "interior dots",
                id: "user.name",
            },
            ValidTenantCase {
                description: "one byte",
                id: "a",
            },
            ValidTenantCase {
                description: "upper case",
                id: "ALL-CAPS",
            },
            ValidTenantCase {
                description: "every punctuation mark",
                id: "ascii!-_.*'()",
            },
            ValidTenantCase {
                description: "digits only",
                id: "0123456789",
            },
            ValidTenantCase {
                description: "leading dot",
                id: ".hidden",
            },
            ValidTenantCase {
                description: "three dots",
                id: "...",
            },
            ValidTenantCase {
                description: "anonymous",
                id: ANONYMOUS_TENANT,
            },
        ];

        for case in valid {
            let parsed = TenantId::new(case.id);
            check!(
                parsed.as_ref().map(TenantId::as_str) == Ok(case.id),
                "{}",
                case.description
            );
        }
    }

    /// Every message is the text the pinned Mimir 2.16.1 and Loki 3.5.1 images
    /// send for the same `X-Scope-OrgID`, captured from the running containers.
    /// A Grafana datasource shows that text to its user, so a rewording is a
    /// visible divergence and not a refactor.
    #[test]
    fn every_invalid_shape_is_rejected_with_upstreams_message() {
        let too_long = "x".repeat(MAX_TENANT_ID_LEN + 1);
        let too_long_and_bad = format!("{too_long}/");
        let unsupported = |id: &str, character: char| {
            format!("tenant ID '{id}' contains unsupported character '{character}'")
        };
        let invalid = [
            InvalidTenantCase {
                description: "empty",
                id: "",
                expected: "tenant ID is empty".to_string(),
            },
            InvalidTenantCase {
                description: "one over the limit",
                id: too_long.as_str(),
                expected: "tenant ID is too long: max 150 characters".to_string(),
            },
            InvalidTenantCase {
                description: "dot",
                id: ".",
                expected: "tenant ID is '.' or '..'".to_string(),
            },
            InvalidTenantCase {
                description: "dot dot",
                id: "..",
                expected: "tenant ID is '.' or '..'".to_string(),
            },
            InvalidTenantCase {
                description: "separator",
                id: "a/b",
                expected: unsupported("a/b", '/'),
            },
            InvalidTenantCase {
                description: "traversal",
                id: "../other",
                expected: unsupported("../other", '/'),
            },
            InvalidTenantCase {
                description: "absolute",
                id: "/etc/passwd",
                expected: unsupported("/etc/passwd", '/'),
            },
            InvalidTenantCase {
                description: "backslash",
                id: "a\\b",
                expected: unsupported("a\\b", '\\'),
            },
            InvalidTenantCase {
                description: "space",
                id: "with space",
                expected: unsupported("with space", ' '),
            },
            InvalidTenantCase {
                description: "leading space",
                id: " lead",
                expected: unsupported(" lead", ' '),
            },
            InvalidTenantCase {
                description: "trailing space",
                id: "trail ",
                expected: unsupported("trail ", ' '),
            },
            InvalidTenantCase {
                description: "tab",
                id: "a\tb",
                expected: unsupported("a\tb", '\t'),
            },
            InvalidTenantCase {
                description: "null",
                id: "a\0b",
                expected: unsupported("a\0b", '\0'),
            },
            InvalidTenantCase {
                description: "delete",
                id: "ctl\u{7f}",
                expected: unsupported("ctl\u{7f}", '\u{7f}'),
            },
            // `dskit` names the first byte of `é`, read as a code point, and so
            // does not name `é`. The pinned Mimir image answers `Ã` here.
            InvalidTenantCase {
                description: "non ASCII",
                id: "unicode-\u{e9}",
                expected: unsupported("unicode-\u{e9}", '\u{c3}'),
            },
            InvalidTenantCase {
                description: "euro sign",
                id: "a\u{20ac}b",
                expected: unsupported("a\u{20ac}b", '\u{e2}'),
            },
            InvalidTenantCase {
                description: "percent",
                id: "a%2Fb",
                expected: unsupported("a%2Fb", '%'),
            },
            InvalidTenantCase {
                description: "comma",
                id: "comma,tenant",
                expected: unsupported("comma,tenant", ','),
            },
            InvalidTenantCase {
                description: "pipe",
                id: "a|b",
                expected: unsupported("a|b", '|'),
            },
            // `dskit` checks the characters before the length, so an id that
            // breaks both rules names the character.
            InvalidTenantCase {
                description: "too long and unsupported",
                id: too_long_and_bad.as_str(),
                expected: unsupported(&too_long_and_bad, '/'),
            },
        ];

        for case in invalid {
            check!(
                TenantId::new(case.id).map_err(|error| error.to_string()) == Err(case.expected),
                "{}",
                case.description
            );
        }
    }

    #[test]
    fn the_length_limit_is_a_boundary_and_not_a_range() {
        check!(TenantId::new("x".repeat(MAX_TENANT_ID_LEN)).is_ok());
        check!(TenantId::new("x".repeat(MAX_TENANT_ID_LEN + 1)) == Err(TenantIdError::TooLong));
        check!(MAX_TENANT_ID_LEN == 150);
    }

    #[test]
    fn the_anonymous_tenant_is_itself_a_tenant_id() {
        let anonymous = TenantId::new(ANONYMOUS_TENANT).expect("anonymous is a valid tenant id");
        assert!(TenantId::anonymous() == anonymous);
        assert!(anonymous.as_str() == "anonymous");
        assert!(anonymous.to_string() == "anonymous");
        assert!(anonymous.into_string() == "anonymous");
    }

    #[test]
    fn parsing_from_a_string_runs_the_same_rules() {
        check!("tenant-a".parse::<TenantId>().map(TenantId::into_string) == Ok("tenant-a".into()));
        check!(
            "a/b".parse::<TenantId>()
                == Err(TenantIdError::UnsupportedCharacter {
                    tenant: "a/b".into(),
                    character: '/',
                })
        );
    }

    #[test]
    fn serde_carries_the_bare_name_and_validates_on_the_way_in() {
        let tenant = TenantId::new("tenant-a").expect("tenant-a is a valid tenant id");
        let json = serde_json::to_string(&tenant).expect("a tenant id serialises");
        assert!(json == "\"tenant-a\"");
        check!(serde_json::from_str::<TenantId>(&json).ok() == Some(tenant));
        check!(serde_json::from_str::<TenantId>("\"a/b\"").is_err());
        check!(serde_json::from_str::<TenantId>("\"..\"").is_err());
        check!(serde_json::from_str::<TenantId>("\"\"").is_err());
    }

    /// Every boundary resolves a tenant through this one function, so each row
    /// is a request shape a client can send, under both policies. An absent and
    /// an empty value must answer alike, and a malformed value must never fall
    /// back to the default tenant: that would put a client's data under a
    /// tenant it did not name.
    #[test]
    fn resolution_applies_the_policy_only_to_a_request_without_a_tenant() {
        let fallback = TenantPolicy::Fallback(TenantId::new("single").unwrap());
        let tenant_a = Ok(TenantId::new("tenant-a").unwrap());
        let single = Ok(TenantId::new("single").unwrap());
        let unsupported = |tenant: &str, character| {
            Err(TenantResolveError::Invalid(
                TenantIdError::UnsupportedCharacter {
                    tenant: tenant.into(),
                    character,
                },
            ))
        };

        let cases = [
            TenantResolutionCase {
                name: "named",
                value: Some(b"tenant-a"),
                required: tenant_a.clone(),
                fallback: tenant_a,
            },
            TenantResolutionCase {
                name: "absent",
                value: None,
                required: Err(TenantResolveError::Missing),
                fallback: single.clone(),
            },
            TenantResolutionCase {
                name: "empty",
                value: Some(b""),
                required: Err(TenantResolveError::Missing),
                fallback: single,
            },
            TenantResolutionCase {
                name: "malformed",
                value: Some(b"a/b"),
                required: unsupported("a/b", '/'),
                fallback: unsupported("a/b", '/'),
            },
            TenantResolutionCase {
                name: "not UTF-8",
                value: Some(b"a\xff"),
                required: unsupported("a\u{fffd}", '\u{ff}'),
                fallback: unsupported("a\u{fffd}", '\u{ff}'),
            },
        ];
        for case in cases {
            check!(
                TenantId::resolve(case.value, &TenantPolicy::Required) == case.required,
                "{}",
                case.name
            );
            check!(
                TenantId::resolve(case.value, &fallback) == case.fallback,
                "{}",
                case.name
            );
        }
    }

    #[test]
    fn the_anonymous_policy_falls_back_to_the_anonymous_tenant() {
        assert!(
            TenantId::resolve(None, &TenantPolicy::anonymous()).map(TenantId::into_string)
                == Ok(ANONYMOUS_TENANT.to_string())
        );
    }

    #[test]
    fn a_missing_tenant_reports_grafanas_message() {
        assert!(TenantResolveError::Missing.to_string() == "no org id");
        assert!(
            TenantResolveError::Invalid(TenantIdError::RelativePathSegment).to_string()
                == "tenant ID is '.' or '..'"
        );
    }
}

mod anonymous_tenant;
mod is_allowed_tenant_byte;
mod max_tenant_id_len;
mod tenant_header;
mod tenant_id;
mod tenant_id_error;
mod tenant_policy;
mod tenant_resolve_error;

pub use anonymous_tenant::ANONYMOUS_TENANT;
use is_allowed_tenant_byte::is_allowed_tenant_byte;
pub use max_tenant_id_len::MAX_TENANT_ID_LEN;
pub use tenant_header::TENANT_HEADER;
pub use tenant_id::TenantId;
pub use tenant_id_error::TenantIdError;
pub use tenant_policy::TenantPolicy;
pub use tenant_resolve_error::TenantResolveError;
