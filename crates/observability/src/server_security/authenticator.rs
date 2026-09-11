use std::sync::Arc;

use axum::http::HeaderMap;

use super::{
    AuthFailureReason, PeerAddr, Principal, SecurityEventSink, UnauthenticatedRoutes,
    credentials::Credentials, presented_credential::PresentedCredential,
};

/// Everything one listener needs to authenticate a request.
#[derive(Debug)]
pub struct Authenticator {
    pub credentials: Arc<Credentials>,
    pub events: SecurityEventSink,
    pub unauthenticated_routes: UnauthenticatedRoutes,
}

impl Authenticator {
    /// Finds the principal of one request, and reports the result to the security events.
    ///
    /// `None` means the request failed. The caller answers every failure the
    /// same way, so that the answer does not say which part was wrong.
    pub fn authenticate(&self, headers: &HeaderMap, peer: Option<&PeerAddr>) -> Option<Principal> {
        let source = peer.map(|peer| peer.socket);
        let events = self.events.events();
        let credential = match PresentedCredential::from_request(headers, peer) {
            Ok(credential) => credential,
            Err((attempted, reason)) => {
                events.authentication_failed(source, attempted, reason);
                return None;
            }
        };
        let method = credential.method();
        let principal = match credential {
            PresentedCredential::Bearer(token) => self
                .credentials
                .principal_for_token(token)
                .ok_or(AuthFailureReason::UnknownCredential),
            PresentedCredential::Basic { username, token } => self
                .credentials
                .principal_for_basic(&username, &token)
                .ok_or(AuthFailureReason::UnknownCredential),
            PresentedCredential::ClientCertificate(identity) => {
                self.credentials.principal_for_client_identity(identity)
            }
        };
        match principal {
            Ok(principal) => {
                events.authentication_succeeded(source, &principal.name, method);
                Some(Principal::Authenticated {
                    name: principal.name.clone(),
                    method,
                    tenants: principal.tenants.clone(),
                    admin: principal.admin,
                    events: self.events.clone(),
                })
            }
            Err(reason) => {
                events.authentication_failed(source, Some(method), reason);
                None
            }
        }
    }
}
