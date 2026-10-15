//! TLS, authentication and tenant authorization for every Krabka listener.
//!
//! Grafana Mimir, Loki, Tempo and Pyroscope ship without authentication, and
//! they turn on TLS through the `-server.http-tls-*` flags. Krabka uses the
//! same opt-in posture. A service with no security flags serves plain HTTP,
//! accepts every request, and logs one warning when its listener starts.
//!
//! Each part turns on independently:
//!
//! - TLS turns on when `--server-tls-cert-path` is set. Client certificates
//!   turn on with `--server-tls-client-auth` and `--server-tls-client-ca-path`.
//! - Authentication turns on when `--auth-credentials-config` names a
//!   credentials file.
//! - Tenant authorization follows authentication. Each signal resolves its
//!   tenant, and then calls [`authorize_tenant`] with the request's
//!   [`Principal`].
//!
//! # Wiring a listener
//!
//! A service binary flattens [`ServerSecurityArgs`] into its CLI and calls
//! [`ServerSecurityArgs::load`] once. It then gives the loaded
//! [`ServerSecurity`] to every listener:
//!
//! ```no_run
//! # use krabka_observability::server_security::{ServerListener, ServerSecurity, serve_router};
//! # async fn f(security: ServerSecurity, router: axum::Router) -> Result<(), Box<dyn std::error::Error>> {
//! let tcp = tokio::net::TcpListener::bind("127.0.0.1:3100").await?;
//! let listener = ServerListener::bind(tcp, &security)?;
//! serve_router(listener, router, &security)
//!     .with_graceful_shutdown(async {})
//!     .await?;
//! # Ok(())
//! # }
//! ```
//!
//! A handler reads the [`Principal`] from its request extensions, and the
//! connection's [`PeerAddr`] from `ConnectInfo<PeerAddr>`. The standalone
//! gRPC servers use [`grpc_incoming`] and [`GrpcAuthenticationLayer`] instead.
//!
//! # The credentials file
//!
//! The file is YAML. An unknown key is an error.
//!
//! ```yaml
//! principals:
//!   - name: grafana
//!     # SHA-256 hex digests of bearer tokens. The token is also accepted as the
//!     # basic-auth password when the basic-auth username equals `name`.
//!     token_sha256: ["6dbe357e39391f70ee29ea8971eaeec10d6d52bce8536e34469ee62c56a7b7fb"]
//!     # Verified client-certificate identities (CN or SAN) for this principal.
//!     client_certificates: ["grafana.internal"]
//!     tenants: ["tenant-a", "tenant-b"] # or ["*"] for every tenant
//!     admin: false # true lets it call ops mutations such as POST /log_level
//! ```
//!
//! `name` and `tenants` are required. `token_sha256` and
//! `client_certificates` default to empty lists, and `admin` defaults to
//! `false`. Loading rejects a file that breaks one of these rules:
//!
//! - there is at least one principal, and every principal has at least one
//!   token digest or certificate identity;
//! - every name is non-empty and unique;
//! - every tenant is a valid [`krabka_blockstore::TenantId`], or the list is
//!   exactly `["*"]`;
//! - every digest is exactly 64 lowercase hexadecimal characters;
//! - no token digest and no certificate identity belongs to two principals.
//!
//! **An unsalted SHA-256 digest is safe only for a high-entropy token.** A
//! short or guessable token falls to an offline search of its digest. Make
//! every token from at least 32 random bytes, and store only its digest:
//!
//! ```sh
//! TOKEN="$(openssl rand -hex 32)"
//! printf %s "$TOKEN" | sha256sum
//! ```
//!
//! # Where authentication does not apply
//!
//! [`UnauthenticatedRoutes`] lists the routes that skip authentication. The
//! default is `GET /ready` and `GET /metrics`, because a Kubernetes probe and a
//! Prometheus scrape carry no credentials. Mimir and Loki also serve both
//! outside their authentication middleware.
//!
//! A request that sends a credential to a service with no credentials file is
//! served, and the service ignores the credential.

use std::path::Path;

mod admin_denied;
mod authenticator;
mod authentication;
mod authorize_admin;
mod authorize_tenant;
mod client_auth;
mod client_identity;
mod credentials;
mod credentials_error;
mod grpc_authentication;
mod grpc_authentication_layer;
mod grpc_connection;
mod grpc_incoming;
mod internal_client;
mod listener_mode;
mod load_security_config;
mod tls_accept;
mod no_security_events;
mod pem;
mod peer_addr;
mod presented_credential;
mod principal;
mod security_event_sink;
mod security_events;
mod serve_router;
mod server_listener;
mod server_security_args;
mod server_security_error;
mod server_security_settings;
mod server_stream;
mod server_tls;
mod spawn_router_server;
mod tenant_denied;
mod tenant_grant;
#[cfg(test)]
mod tests;
mod token_digest;
mod unauthenticated_routes;
mod unauthorized_response;
mod warn_about_posture;

pub use self::{
    admin_denied::AdminDenied,
    authentication::authenticate_requests,
    authorize_admin::authorize_admin,
    authorize_tenant::authorize_tenant,
    client_auth::ClientAuth,
    client_identity::ClientIdentity,
    credentials_error::CredentialsError,
    grpc_authentication::GrpcAuthentication,
    grpc_authentication_layer::GrpcAuthenticationLayer,
    grpc_connection::GrpcConnection,
    grpc_incoming::grpc_incoming,
    internal_client::InternalClient,
    no_security_events::NoSecurityEvents,
    peer_addr::PeerAddr,
    principal::Principal,
    security_event_sink::SecurityEventSink,
    security_events::SecurityEvents,
    serve_router::serve_router,
    server_listener::ServerListener,
    server_security_args::ServerSecurityArgs,
    server_security_error::ServerSecurityError,
    server_security_settings::ServerSecurity,
    server_stream::ServerStream,
    spawn_router_server::{
        JoinableRouterServer, RouterServer, spawn_router_server, spawn_router_server_joinable,
        spawn_router_server_until_shutdown,
    },
    tenant_denied::TenantDenied,
    tenant_grant::TenantGrant,
    unauthenticated_routes::UnauthenticatedRoutes,
};

/// Defines the shared `serve` wrapper used by signal distributors.
#[macro_export]
macro_rules! define_distributor_serve {
    ($state:ty, $router:path, $service:literal, $description:literal) => {
        #[doc = $description]
        pub async fn serve(
            addr: std::net::SocketAddr,
            state: std::sync::Arc<$state>,
            security: &$crate::server_security::ServerSecurity,
            shutdown: impl std::future::Future<Output = ()> + Send + 'static,
        ) -> std::io::Result<std::net::SocketAddr> {
            $crate::server_security::spawn_router_server_until_shutdown(
                addr,
                $router(state),
                security,
                shutdown,
                $service,
            )
            .await
        }
    };
}

/// Makes `ring` the process-wide rustls crypto provider, once.
///
/// This workspace compiles rustls with two providers, `ring` and `aws-lc-rs`.
/// With two compiled in, rustls cannot pick one, so any code that asks it for
/// the process default panics on its first TLS connection. That includes the
/// Kafka client, `reqwest`, and tonic. A service binary calls this function
/// first, before it builds anything that can open a TLS connection.
///
/// A second call, or a call after another provider is installed, does nothing.
/// The listeners in this module do not depend on it, because they name `ring`
/// themselves.
pub fn install_crypto_provider() {
    // An `Err` means a provider is installed already, which is the state this
    // function exists to reach.
    let _ = rustls::crypto::ring::default_provider().install_default();
}

/// Reads a whole file, with an error that names it.
fn read_file(path: &Path) -> Result<Vec<u8>, ServerSecurityError> {
    std::fs::read(path).map_err(|source| ServerSecurityError::ReadFile {
        path: path.to_owned(),
        source,
    })
}

/// Why a request failed authentication, as a category that never holds the credential.
///
/// The 401 response is the same for every reason. Only a
/// [`SecurityEvents`](SecurityEvents) implementation sees the reason.
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

/// The kind of credential that authenticated a request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AuthMethod {
    /// An `Authorization: Bearer <token>` header.
    Bearer,
    /// An `Authorization: Basic <base64 name:token>` header.
    Basic,
    /// The verified client certificate of the TLS connection.
    ClientCertificate,
}
