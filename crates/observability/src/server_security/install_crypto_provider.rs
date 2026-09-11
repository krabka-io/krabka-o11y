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
