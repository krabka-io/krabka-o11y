use std::sync::Arc;

use super::{
    InternalClient, SecurityEventSink, SecurityEvents, UnauthenticatedRoutes,
    authenticator::Authenticator, credentials::Credentials, server_tls::ServerTls,
};

/// The loaded security posture of one service: TLS, authentication and outbound credentials.
///
/// [`ServerSecurityArgs::load`](super::ServerSecurityArgs::load) builds it.
/// `ServerSecurity::default()` is the upstream default: plain HTTP, no
/// authentication, and no outbound credentials. A clone shares the loaded
/// files, so give every listener of the service the same value.
#[derive(Debug, Clone, Default)]
pub struct ServerSecurity {
    tls: Option<ServerTls>,
    credentials: Option<Arc<Credentials>>,
    events: SecurityEventSink,
    unauthenticated_routes: UnauthenticatedRoutes,
    internal_client: InternalClient,
}

impl ServerSecurity {
    pub(super) fn new(
        tls: Option<ServerTls>,
        credentials: Option<Credentials>,
        internal_client: InternalClient,
    ) -> Self {
        Self {
            tls,
            credentials: credentials.map(Arc::new),
            events: SecurityEventSink::default(),
            unauthenticated_routes: UnauthenticatedRoutes::default(),
            internal_client,
        }
    }

    /// The posture with `events` receiving every authentication and authorization decision.
    #[must_use]
    pub fn with_security_events(self, events: Arc<dyn SecurityEvents>) -> Self {
        Self {
            events: SecurityEventSink::new(events),
            ..self
        }
    }

    /// The posture with `routes` in place of the default [`UnauthenticatedRoutes`].
    #[must_use]
    pub fn with_unauthenticated_routes(self, routes: UnauthenticatedRoutes) -> Self {
        Self {
            unauthenticated_routes: routes,
            ..self
        }
    }

    /// Whether the listeners serve TLS.
    #[must_use]
    pub fn tls_enabled(&self) -> bool {
        self.tls.is_some()
    }

    /// Whether a credentials file is configured, so requests need a credential.
    #[must_use]
    pub fn authentication_enabled(&self) -> bool {
        self.credentials.is_some()
    }

    /// The credentials this service presents to other Krabka services.
    #[must_use]
    pub fn internal_client(&self) -> &InternalClient {
        &self.internal_client
    }

    pub(super) fn tls(&self) -> Option<&ServerTls> {
        self.tls.as_ref()
    }

    /// The authenticator for one listener, or `None` without a credentials file.
    pub(super) fn authenticator(&self) -> Option<Arc<Authenticator>> {
        self.credentials.as_ref().map(|credentials| {
            Arc::new(Authenticator {
                credentials: credentials.clone(),
                events: self.events.clone(),
                unauthenticated_routes: self.unauthenticated_routes.clone(),
            })
        })
    }
}
