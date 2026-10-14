use axum::http::{HeaderMap, header};
use base64::{Engine as _, engine::general_purpose::STANDARD};

use super::{AuthFailureReason, AuthMethod, ClientIdentity, PeerAddr};

#[derive(Clone, Copy, Debug)]
pub(super) struct CredentialParseFailure {
    pub(super) method: Option<AuthMethod>,
    pub(super) reason: AuthFailureReason,
}

/// The credential a request presented, before any principal is looked up.
pub enum PresentedCredential<'a> {
    /// The token from `Authorization: Bearer <token>`.
    Bearer(&'a [u8]),
    /// The name and token from `Authorization: Basic <base64 name:token>`.
    Basic { username: String, token: Vec<u8> },
    /// The verified client certificate of the connection.
    ClientCertificate(&'a ClientIdentity),
}

impl<'a> PresentedCredential<'a> {
    /// Reads the credential of one request.
    ///
    /// An `Authorization` header decides when it is present, even when the
    /// connection also has a verified client certificate. A proxy can then
    /// hold one mutual-TLS connection and still send each request as its own
    /// principal. A header that fails does not fall back to the certificate.
    pub fn from_request(
        headers: &'a HeaderMap,
        peer: Option<&'a PeerAddr>,
    ) -> Result<Self, CredentialParseFailure> {
        let mut values = headers.get_all(header::AUTHORIZATION).into_iter();
        let Some(value) = values.next() else {
            return peer
                .and_then(|peer| peer.client_identity.as_deref())
                .map(Self::ClientCertificate)
                .ok_or(CredentialParseFailure {
                    method: None,
                    reason: AuthFailureReason::MissingCredential,
                });
        };
        if values.next().is_some() {
            return Err(CredentialParseFailure {
                method: None,
                reason: AuthFailureReason::MalformedCredential,
            });
        }
        let value = value.as_bytes();
        let space = value.iter().position(|byte| *byte == b' ');
        let scheme = space.map_or(value, |space| &value[..space]);
        let credential = space.map_or(&[][..], |space| value[space..].trim_ascii_start());
        if scheme.eq_ignore_ascii_case(b"bearer") {
            return if credential.is_empty() {
                Err(CredentialParseFailure {
                    method: Some(AuthMethod::Bearer),
                    reason: AuthFailureReason::MalformedCredential,
                })
            } else {
                Ok(Self::Bearer(credential))
            };
        }
        if scheme.eq_ignore_ascii_case(b"basic") {
            let malformed = CredentialParseFailure {
                method: Some(AuthMethod::Basic),
                reason: AuthFailureReason::MalformedCredential,
            };
            let decoded = STANDARD.decode(credential).map_err(|_| malformed)?;
            let colon = decoded
                .iter()
                .position(|byte| *byte == b':')
                .ok_or(malformed)?;
            let username = &decoded[..colon];
            let token = &decoded[colon + 1..];
            let username = std::str::from_utf8(username).map_err(|_| malformed)?;
            if token.is_empty() {
                return Err(malformed);
            }
            return Ok(Self::Basic {
                username: username.to_owned(),
                token: token.to_vec(),
            });
        }
        Err(CredentialParseFailure {
            method: None,
            reason: AuthFailureReason::UnsupportedScheme,
        })
    }

    /// The kind of credential.
    pub fn method(&self) -> AuthMethod {
        match self {
            Self::Bearer(_) => AuthMethod::Bearer,
            Self::Basic { .. } => AuthMethod::Basic,
            Self::ClientCertificate(_) => AuthMethod::ClientCertificate,
        }
    }
}
