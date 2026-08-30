//! Tenant id validation (Mimir/Pyroscope-style `X-Scope-OrgID` charset).
//!
//! The accepted charset mirrors Grafana Mimir's tenant validation: a bounded
//! length, a restricted ASCII charset, and explicit rejection of path-unsafe
//! segments (`.`, `..`, `/`, `\`) so a tenant id can never escape a storage
//! prefix or smuggle control characters.

use crate::error::ProfilesError;

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;

    #[test]
    fn valid_tenant_is_returned() {
        let ok = validate_tenant("tenant-a");
        assert!(ok == Ok("tenant-a".to_string()));
    }

    #[test]
    fn normal_tenant_ok() {
        check!(validate_tenant("tenant-a").is_ok());
        check!(validate_tenant("Team_42!").is_ok());
        check!(validate_tenant("a.b.c").is_ok());
    }

    #[test]
    fn header_none_is_anonymous() {
        assert!(tenant_from_header(None) == Ok("anonymous".to_string()));
    }

    #[test]
    fn header_empty_is_anonymous() {
        assert!(tenant_from_header(Some("")) == Ok("anonymous".to_string()));
    }

    #[test]
    fn header_present_validates() {
        assert!(tenant_from_header(Some("tenant-a")) == Ok("tenant-a".to_string()));
        check!(tenant_from_header(Some("a/b")).is_err());
    }

    #[test]
    fn over_max_length_is_rejected() {
        let long = "a".repeat(MAX_TENANT_LEN + 1);
        check!(validate_tenant(&long).is_err());
        // Exactly at the limit is allowed.
        let at_limit = "a".repeat(MAX_TENANT_LEN);
        check!(validate_tenant(&at_limit).is_ok());
    }

    #[test]
    fn empty_is_rejected() {
        check!(validate_tenant("").is_err());
    }

    #[test]
    fn path_unsafe_segments_are_rejected() {
        check!(validate_tenant("../x").is_err());
        check!(validate_tenant("a/b").is_err());
        check!(validate_tenant("a\\b").is_err());
        check!(validate_tenant("..").is_err());
        check!(validate_tenant(".").is_err());
    }

    #[test]
    fn whitespace_and_control_are_rejected() {
        check!(validate_tenant("a b").is_err());
        check!(validate_tenant("a\tb").is_err());
        check!(validate_tenant(" lead").is_err());
        check!(validate_tenant("trail ").is_err());
        check!(validate_tenant("ctl\u{7f}").is_err());
    }
}

mod anonymous_tenant;
mod is_allowed_byte;
mod max_tenant_len;
mod tenant_from_header;
mod validate_tenant;

pub use anonymous_tenant::ANONYMOUS_TENANT;
use is_allowed_byte::is_allowed_byte;
use max_tenant_len::MAX_TENANT_LEN;
pub use tenant_from_header::tenant_from_header;
pub use validate_tenant::validate_tenant;
