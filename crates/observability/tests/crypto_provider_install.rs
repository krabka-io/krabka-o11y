//! `install_crypto_provider` in a process of its own.
//!
//! The provider is process-global, so this binary holds only tests that
//! install it. `tests/crypto_provider_absent.rs` holds the tests that need
//! none installed.

use krabka_observability::server_security::install_crypto_provider;

/// After the call, code that asks rustls for the process default gets `ring`
/// and does not panic, and a second call changes nothing. The Kafka client,
/// `reqwest` and tonic all ask for the default.
#[test]
fn installing_the_provider_lets_rustls_build_a_config_and_is_idempotent() {
    install_crypto_provider();
    let installed = rustls::crypto::CryptoProvider::get_default()
        .expect("a provider is installed")
        .clone();
    install_crypto_provider();

    assert2::assert!(
        std::panic::catch_unwind(rustls::ServerConfig::builder).is_ok(),
        "rustls builds a config from the installed default"
    );
    let again = rustls::crypto::CryptoProvider::get_default().expect("still installed");
    assert2::assert!(std::sync::Arc::ptr_eq(&installed, again));
}
