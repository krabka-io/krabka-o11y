//! Tenant-ID validation matching Grafana-Mimir `ValidTenantID`.

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::validate_tenant;

    #[test]
    fn valid_and_invalid_tenant_ids() {
        let valid = [
            "tenant-a",
            "team_42",
            "user.name",
            "a",
            "ALL-CAPS",
            "ascii!-_.*'()",
        ];
        for id in valid {
            assert!(validate_tenant(id).is_ok(), "expected `{id}` to be valid");
        }

        let invalid = [
            "",
            ".",
            "..",
            "with space",
            "slash/tenant",
            "comma,tenant",
            "unicode-é",
            "tab\ttenant",
        ];
        for id in invalid {
            assert!(
                validate_tenant(id).is_err(),
                "expected `{id}` to be invalid"
            );
        }

        // Length boundary: exactly 150 bytes is allowed, 151 is rejected.
        assert!(validate_tenant(&"x".repeat(150)).is_ok());
        assert!(validate_tenant(&"x".repeat(151)).is_err());
    }
}

// === split-modules: generated submodules ===
mod is_allowed_tenant_byte;
mod max_tenant_id_len;
mod validate_tenant;

use is_allowed_tenant_byte::is_allowed_tenant_byte;
use max_tenant_id_len::MAX_TENANT_ID_LEN;
pub use validate_tenant::validate_tenant;
