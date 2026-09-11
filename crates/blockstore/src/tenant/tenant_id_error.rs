use super::{Error, MAX_TENANT_ID_LEN};

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
    /// [`super::TenantId::resolve`] never returns this, because an empty
    /// header is a missing tenant there. Only [`super::TenantId::new`] does.
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
