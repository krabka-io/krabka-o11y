use std::fmt;

use axum::http::{HeaderMap, HeaderValue, header};

/// The credentials that one Krabka service presents when it calls another.
///
/// A traces query frontend that calls a querier, or a metrics-generator that
/// remote-writes to a distributor, applies this to its `reqwest` client.
/// [`ServerSecurityArgs::load`](super::ServerSecurityArgs::load) builds it
/// from the `--internal-client-*` flags. With no such flag, it changes nothing.
#[derive(Clone, Default)]
pub struct InternalClient {
    authorization: Option<HeaderValue>,
    identity: Option<reqwest::Identity>,
    trusted_roots: Vec<reqwest::Certificate>,
}

impl InternalClient {
    pub(super) fn new(
        authorization: Option<HeaderValue>,
        identity: Option<reqwest::Identity>,
        trusted_roots: Vec<reqwest::Certificate>,
    ) -> Self {
        Self {
            authorization,
            identity,
            trusted_roots,
        }
    }

    /// Adds the configured credentials to `builder`.
    ///
    /// - A token becomes a default `Authorization: Bearer <token>` header,
    ///   marked sensitive so that it stays out of debug output.
    /// - A client certificate and key become the client's TLS identity.
    /// - A CA bundle becomes the only set of roots that the client trusts.
    ///   Service-to-service calls go to Krabka listeners, so the system roots
    ///   are not needed, and a public CA cannot stand in for an internal one.
    pub fn apply(&self, builder: reqwest::ClientBuilder) -> reqwest::ClientBuilder {
        let mut builder = builder;
        if let Some(authorization) = &self.authorization {
            let mut headers = HeaderMap::new();
            headers.insert(header::AUTHORIZATION, authorization.clone());
            builder = builder.default_headers(headers);
        }
        if let Some(identity) = &self.identity {
            builder = builder.identity(identity.clone());
        }
        if !self.trusted_roots.is_empty() {
            builder = builder.tls_certs_only(self.trusted_roots.iter().cloned());
        }
        builder
    }

    /// Whether any `--internal-client-*` flag was set.
    #[must_use]
    pub fn is_configured(&self) -> bool {
        self.authorization.is_some() || self.identity.is_some() || !self.trusted_roots.is_empty()
    }
}

impl fmt::Debug for InternalClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InternalClient")
            .field("token", &self.authorization.as_ref().map(|_| "<redacted>"))
            .field("identity", &self.identity.is_some())
            .field("trusted_roots", &self.trusted_roots.len())
            .finish()
    }
}
