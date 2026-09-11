/// Why a request failed authentication, as a category that never holds the credential.
///
/// The 401 response is the same for every reason. Only a
/// [`SecurityEvents`](super::SecurityEvents) implementation sees the reason.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AuthFailureReason {
    /// The request sent no `Authorization` header, and its connection has no
    /// verified client certificate.
    MissingCredential,
    /// The `Authorization` header uses a scheme other than `Bearer` or `Basic`.
    UnsupportedScheme,
    /// The `Authorization` header could not be decoded, is empty, or appears
    /// more than once.
    MalformedCredential,
    /// The credential matched no principal.
    UnknownCredential,
    /// The client certificate names identities of more than one principal.
    AmbiguousClientCertificate,
}
