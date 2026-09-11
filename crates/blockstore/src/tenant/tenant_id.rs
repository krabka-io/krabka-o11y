use super::{
    ANONYMOUS_TENANT, DeError, Deserialize, Deserializer, Display, FromStr, MAX_TENANT_ID_LEN,
    Serialize, TenantIdError, TenantPolicy, TenantResolveError, is_allowed_tenant_byte,
};

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
