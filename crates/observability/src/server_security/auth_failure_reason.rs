use krabka_domain_macros::EnumName;

/// Why a request failed authentication, as a category that never holds the credential.
///
/// The 401 response is the same for every reason. Only a
/// [`SecurityEvents`](super::SecurityEvents) implementation sees the reason.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, EnumName)]
#[enum_name(accessor = "failure_reason")]
pub enum AuthFailureReason {
    /// The request sent no `Authorization` header, and its connection has no
    /// verified client certificate.
    #[name(value = "missing_credential")]
    MissingCredential,
    /// The `Authorization` header uses a scheme other than `Bearer` or `Basic`.
    #[name(value = "unsupported_scheme")]
    UnsupportedScheme,
    /// The `Authorization` header could not be decoded, is empty, or appears
    /// more than once.
    #[name(value = "malformed_credential")]
    MalformedCredential,
    /// The credential matched no principal.
    #[name(value = "unknown_credential")]
    UnknownCredential,
    /// The client certificate names identities of more than one principal.
    #[name(value = "ambiguous_client_certificate")]
    AmbiguousClientCertificate,
}
