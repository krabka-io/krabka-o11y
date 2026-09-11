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

    #[test]
    fn every_valid_shape_is_accepted() {
        let valid = [
            ("plain", "tenant-a"),
            ("underscore", "team_42"),
            ("interior dots", "user.name"),
            ("one byte", "a"),
            ("upper case", "ALL-CAPS"),
            ("every punctuation mark", "ascii!-_.*'()"),
            ("digits only", "0123456789"),
            ("leading dot", ".hidden"),
            ("three dots", "..."),
            ("anonymous", ANONYMOUS_TENANT),
        ];

        for (name, id) in valid {
            let parsed = TenantId::new(id);
            check!(parsed.as_ref().map(TenantId::as_str) == Ok(id), "{name}");
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
            ("empty", "", "tenant ID is empty".to_string()),
            (
                "one over the limit",
                too_long.as_str(),
                "tenant ID is too long: max 150 characters".to_string(),
            ),
            ("dot", ".", "tenant ID is '.' or '..'".to_string()),
            ("dot dot", "..", "tenant ID is '.' or '..'".to_string()),
            ("separator", "a/b", unsupported("a/b", '/')),
            ("traversal", "../other", unsupported("../other", '/')),
            ("absolute", "/etc/passwd", unsupported("/etc/passwd", '/')),
            ("backslash", "a\\b", unsupported("a\\b", '\\')),
            ("space", "with space", unsupported("with space", ' ')),
            ("leading space", " lead", unsupported(" lead", ' ')),
            ("trailing space", "trail ", unsupported("trail ", ' ')),
            ("tab", "a\tb", unsupported("a\tb", '\t')),
            ("null", "a\0b", unsupported("a\0b", '\0')),
            ("delete", "ctl\u{7f}", unsupported("ctl\u{7f}", '\u{7f}')),
            // `dskit` names the first byte of `é`, read as a code point, and so
            // does not name `é`. The pinned Mimir image answers `Ã` here.
            (
                "non ASCII",
                "unicode-\u{e9}",
                unsupported("unicode-\u{e9}", '\u{c3}'),
            ),
            (
                "euro sign",
                "a\u{20ac}b",
                unsupported("a\u{20ac}b", '\u{e2}'),
            ),
            ("percent", "a%2Fb", unsupported("a%2Fb", '%')),
            ("comma", "comma,tenant", unsupported("comma,tenant", ',')),
            ("pipe", "a|b", unsupported("a|b", '|')),
            // `dskit` checks the characters before the length, so an id that
            // breaks both rules names the character.
            (
                "too long and unsupported",
                too_long_and_bad.as_str(),
                unsupported(&too_long_and_bad, '/'),
            ),
        ];

        for (name, id, expected) in invalid {
            check!(
                TenantId::new(id).map_err(|error| error.to_string()) == Err(expected),
                "{name}"
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

        let cases: [(&str, Option<&[u8]>, _, _); 5] = [
            ("named", Some(b"tenant-a"), tenant_a.clone(), tenant_a),
            (
                "absent",
                None,
                Err(TenantResolveError::Missing),
                single.clone(),
            ),
            ("empty", Some(b""), Err(TenantResolveError::Missing), single),
            (
                "malformed",
                Some(b"a/b"),
                unsupported("a/b", '/'),
                unsupported("a/b", '/'),
            ),
            (
                "not UTF-8",
                Some(b"a\xff"),
                unsupported("a\u{fffd}", '\u{ff}'),
                unsupported("a\u{fffd}", '\u{ff}'),
            ),
        ];
        for (name, value, required, fallen_back) in cases {
            check!(
                TenantId::resolve(value, &TenantPolicy::Required) == required,
                "{name}"
            );
            check!(TenantId::resolve(value, &fallback) == fallen_back, "{name}");
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
