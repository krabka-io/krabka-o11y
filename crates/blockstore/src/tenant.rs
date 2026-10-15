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

/// Longest tenant id in bytes, as Grafana Mimir's `errTenantIDTooLong` sets it.
pub const MAX_TENANT_ID_LEN: usize = 150;

/// The HTTP header, and the gRPC metadata key, that names a request's tenant.
///
/// HTTP header names are case-insensitive, and gRPC metadata keys are
/// lowercase, so this lowercase spelling matches both.
pub const TENANT_HEADER: &str = "x-scope-orgid";

/// Reports whether `byte` is allowed in a tenant id.
///
/// The set is ASCII alphanumerics plus `! - _ . * ' ( )`, which is what
/// Grafana Mimir's `tenant.ValidTenantID` allows.
pub(super) fn is_allowed_tenant_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric()
        || matches!(byte, b'!' | b'-' | b'_' | b'.' | b'*' | b'\'' | b'(' | b')')
}

/// The tenant a single-tenant deployment uses when a request names none.
///
/// Grafana Tempo and Grafana Pyroscope both use this name when multi-tenancy
/// is off, so a Krabka service that accepts a request without an
/// `X-Scope-OrgID` header reads as those components do. It is a valid
/// [`TenantId`], and each signal decides for itself whether a request
/// without a tenant gets this name or a rejection.
pub const ANONYMOUS_TENANT: &str = "anonymous";

/// Why a string is not a tenant id.
///
/// Each message is the text Grafana's `dskit` `tenant.ValidTenantID` writes,
/// which is what Mimir and Loki send back to a client. The pinned Mimir 2.16.1
/// and Loki 3.5.1 images answer a malformed `X-Scope-OrgID` with exactly these
/// strings, and a Grafana datasource shows the string to its user, so Krabka
/// uses the same words. The message repeats the rejected id, as upstream does.
/// A header value cannot hold a line break, so the id cannot forge a second line.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum TenantIdError {
    /// The string has no bytes.
    ///
    /// [`TenantId::resolve`] never returns this, because an empty
    /// header is a missing tenant there. Only [`TenantId::new`] does.
    #[error("tenant ID is empty")]
    Empty,

    /// The string holds a character outside the allowed set.
    #[error("tenant ID '{tenant}' contains unsupported character '{character}'")]
    UnsupportedCharacter {
        /// The rejected id. Bytes that are not UTF-8 appear as U+FFFD.
        tenant: String,
        /// The first character outside the allowed set.
        character: char,
    },

    /// The string is longer than [`MAX_TENANT_ID_LEN`] bytes.
    #[error("tenant ID is too long: max {MAX_TENANT_ID_LEN} characters")]
    TooLong,

    /// The string is exactly `.` or exactly `..`.
    #[error("tenant ID is '.' or '..'")]
    RelativePathSegment,
}

/// What a service does with a request that names no tenant.
///
/// Each Grafana component makes this choice through its multi-tenancy switch.
/// Mimir and Loki default the switch on, and reject a request without an
/// `X-Scope-OrgID` header. Tempo and Pyroscope default it off, and put every
/// such request under one fixed tenant. A Krabka service picks the variant its
/// upstream uses, and [`TenantId::resolve`] applies it the same way at every
/// header and metadata boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TenantPolicy {
    /// A request without a tenant is rejected.
    Required,
    /// A request without a tenant is served as this tenant.
    Fallback(TenantId),
}

impl TenantPolicy {
    /// The fallback policy that uses [`ANONYMOUS_TENANT`].
    #[must_use]
    pub fn anonymous() -> Self {
        Self::Fallback(TenantId::anonymous())
    }
}

/// Why a request does not resolve to a tenant.
///
/// The variants are separate because upstream reports them differently. A
/// missing tenant is Grafana's `no org id` error, and a malformed one names the
/// rule the id broke.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum TenantResolveError {
    /// The request names no tenant, and the policy is [`TenantPolicy::Required`].
    #[error("no org id")]
    Missing,

    /// The request names a tenant that is not a valid tenant id.
    #[error(transparent)]
    Invalid(#[from] TenantIdError),
}

/// A tenant id that is known to be well formed.
///
/// Every Krabka signal scopes its blocks, its index and its queries by tenant,
/// and the tenant name reaches object keys and index prefixes as a path
/// segment. The value comes from the `X-Scope-OrgID` header of an untrusted
/// request, so a name that carries a separator or a relative path segment
/// would write one tenant's data under another tenant's prefix.
///
/// [`TenantId::new`] is the only way to build one, so an instance is proof
/// that the name passed the rules Grafana Mimir's `tenant.ValidTenantID`
/// applies:
///
/// - it is not empty,
/// - it is at most [`MAX_TENANT_ID_LEN`] bytes,
/// - it is not exactly `.` and not exactly `..`,
/// - every byte is an ASCII alphanumeric or one of `! - _ . * ' ( )`.
///
/// The type has no `From<String>`, because an infallible conversion would step
/// around those rules.
///
/// A valid id is still not a path segment. Five of the allowed characters need
/// an escape before they go into an object key. See
/// [`crate::escape_object_path_segment`].
///
/// `Deserialize` runs the same rules, so a tenant id read out of JSON is as
/// trustworthy as one built in code.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Display, Serialize)]
#[serde(transparent)]
pub struct TenantId(String);

impl TenantId {
    /// Builds a tenant id from `id`, or reports why `id` is not one.
    ///
    /// # Errors
    ///
    /// Returns [`TenantIdError::Empty`] for an empty string. Otherwise it
    /// returns the first rule the id breaks, in the order `dskit` checks them:
    /// [`TenantIdError::UnsupportedCharacter`] for a character outside the
    /// allowed set, [`TenantIdError::TooLong`] above [`MAX_TENANT_ID_LEN`]
    /// bytes, and [`TenantIdError::RelativePathSegment`] for `.` and `..`.
    pub fn new(id: impl Into<String>) -> Result<Self, TenantIdError> {
        let id = id.into();
        if id.is_empty() {
            return Err(TenantIdError::Empty);
        }
        // The checks run in `dskit`'s order, so an id that breaks two rules
        // gets the message upstream gives it: the character first, then the
        // length, then the relative path segment.
        if let Some(character) = first_unsupported_character(id.as_bytes()) {
            return Err(TenantIdError::UnsupportedCharacter {
                tenant: id,
                character,
            });
        }
        if id.len() > MAX_TENANT_ID_LEN {
            return Err(TenantIdError::TooLong);
        }
        if id == "." || id == ".." {
            return Err(TenantIdError::RelativePathSegment);
        }
        Ok(Self(id))
    }

    /// The [`ANONYMOUS_TENANT`] id.
    ///
    /// This builds the id without the checks in [`TenantId::new`], because the
    /// name is a constant that passes them. A unit test holds that fact.
    #[must_use]
    pub fn anonymous() -> Self {
        Self(ANONYMOUS_TENANT.to_owned())
    }

    /// Resolves the tenant a request names, under `policy`.
    ///
    /// `value` is the raw `X-Scope-OrgID` header or gRPC metadata value, or
    /// `None` when the request carries neither. An empty value counts as no
    /// value, as it does in Grafana's `dskit`, so a client that sends the header
    /// with nothing in it gets the same answer as one that omits it.
    ///
    /// # Errors
    ///
    /// Returns [`TenantResolveError::Missing`] when the request names no tenant
    /// and `policy` is [`TenantPolicy::Required`], and
    /// [`TenantResolveError::Invalid`] when the value is not a valid tenant id.
    /// A value that is not UTF-8 is invalid, because every allowed byte is ASCII.
    pub fn resolve(
        value: Option<&[u8]>,
        policy: &TenantPolicy,
    ) -> Result<Self, TenantResolveError> {
        match value.filter(|value| !value.is_empty()) {
            Some(value) => match std::str::from_utf8(value) {
                Ok(id) => Ok(Self::new(id)?),
                // A Rust string cannot hold these bytes, so the id in the
                // message is the lossy form, where Mimir repeats the raw bytes.
                // The character is still taken from the raw bytes, so it is
                // the one Mimir names.
                Err(_) => Err(TenantIdError::UnsupportedCharacter {
                    tenant: String::from_utf8_lossy(value).into_owned(),
                    character: first_unsupported_character(value)
                        .unwrap_or(char::REPLACEMENT_CHARACTER),
                }
                .into()),
            },
            None => match policy {
                TenantPolicy::Required => Err(TenantResolveError::Missing),
                TenantPolicy::Fallback(tenant) => Ok(tenant.clone()),
            },
        }
    }

    /// The name, for the many signatures that still take a `&str` tenant.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Takes the name out of the wrapper.
    #[must_use]
    pub fn into_string(self) -> String {
        self.0
    }
}

impl FromStr for TenantId {
    type Err = TenantIdError;

    fn from_str(id: &str) -> Result<Self, Self::Err> {
        Self::new(id)
    }
}

impl<'de> Deserialize<'de> for TenantId {
    // Hand-written rather than derived: a derived `Deserialize` on a
    // `#[serde(transparent)]` newtype skips the constructor, and an instance
    // would then no longer be proof that the name is well formed.
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::new(String::deserialize(deserializer)?).map_err(DeError::custom)
    }
}

/// The first byte outside the allowed set, read as the code point of that byte.
///
/// `dskit` names the byte at the position of the first unsupported rune, and Go
/// prints that byte with `%c`, which reads it as a code point. So `unicode-é`
/// names `Ã`, which is U+00C3 and the first byte of `é`, and not `é` itself. The
/// pinned Mimir 2.16.1 image answers that way. Every allowed byte is ASCII, so
/// the first byte outside the set is also the first byte of that rune.
fn first_unsupported_character(id: &[u8]) -> Option<char> {
    id.iter()
        .copied()
        .find(|byte| !is_allowed_tenant_byte(*byte))
        .map(char::from)
}
