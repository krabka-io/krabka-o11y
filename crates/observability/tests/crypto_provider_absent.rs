//! What rustls and tonic do in this build when no process-wide crypto provider
//! is installed.
//!
//! The provider is process-global, so these proofs live in their own test
//! binary. A unit test elsewhere installs `ring`, and in a shared process that
//! would make these tests pass without proving anything. Nothing in this file
//! installs a provider.

use std::panic::catch_unwind;

use tonic::transport::{Identity, Server, ServerTlsConfig};

/// The workspace compiles rustls with both `ring` and `aws-lc-rs`. With two
/// providers compiled in and none installed, rustls cannot pick one, so its
/// own `builder()` panics. This is why every Krabka TLS config names `ring`,
/// and why each service binary calls `install_crypto_provider` first.
#[test]
fn rustls_cannot_pick_a_provider_on_its_own() {
    assert2::assert!(rustls::crypto::CryptoProvider::get_default().is_none());

    assert2::assert!(catch_unwind(rustls::ServerConfig::builder).is_err());
}

/// tonic's `ServerTlsConfig` builds its rustls config with `builder()`, so it
/// panics in this build too. The gRPC servers take TLS from
/// `server_security::grpc_incoming` for that reason. If this test starts to
/// fail, tonic has fixed it and `ServerTlsConfig` is an option again.
#[test]
fn tonic_server_tls_panics_without_an_installed_provider() {
    assert2::assert!(rustls::crypto::CryptoProvider::get_default().is_none());
    let certified = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()])
        .expect("a self-signed certificate");
    let identity = Identity::from_pem(certified.cert.pem(), certified.signing_key.serialize_pem());
    let config = ServerTlsConfig::new().identity(identity);

    let built = catch_unwind(|| Server::builder().tls_config(config).is_ok());

    assert2::assert!(built.is_err());
}
