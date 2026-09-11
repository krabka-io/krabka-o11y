use std::sync::Arc;

use rustls::{RootCertStore, ServerConfig, server::WebPkiClientVerifier};

use super::{
    ClientAuth, ServerSecurityArgs, ServerSecurityError, read_certificates::read_certificates,
    read_private_key::read_private_key, server_tls::ServerTls,
};

/// Checks the `--server-tls-*` flags together, and builds the TLS configuration they describe.
///
/// Returns `None` when no TLS flag is set.
pub fn load_server_tls(
    args: &ServerSecurityArgs,
) -> Result<Option<ServerTls>, ServerSecurityError> {
    let client_auth = args.server_tls_client_auth;
    let client_ca = args.server_tls_client_ca_path.as_deref();
    let (certificate_path, key_path) = match (
        args.server_tls_cert_path.as_deref(),
        args.server_tls_key_path.as_deref(),
    ) {
        (Some(certificate), Some(key)) => (certificate, key),
        (Some(_), None) => return Err(ServerSecurityError::CertificateWithoutKey),
        (None, Some(_)) => return Err(ServerSecurityError::KeyWithoutCertificate),
        (None, None) if client_ca.is_some() => {
            return Err(ServerSecurityError::ClientCaWithoutCertificate);
        }
        (None, None) if client_auth != ClientAuth::NoClientCert => {
            return Err(ServerSecurityError::ClientAuthWithoutCertificate { client_auth });
        }
        (None, None) => return Ok(None),
    };
    let client_ca = match (client_auth, client_ca) {
        (ClientAuth::NoClientCert, None) => None,
        (ClientAuth::NoClientCert, Some(_)) => {
            return Err(ServerSecurityError::ClientCaWithoutClientAuth);
        }
        (_, None) => return Err(ServerSecurityError::ClientAuthWithoutClientCa { client_auth }),
        (_, Some(path)) => Some(path),
    };

    let chain = read_certificates(certificate_path)?;
    let key = read_private_key(key_path)?;

    // Both `ring` and `aws-lc-rs` are compiled into this workspace. The
    // workspace `rustls` entry turns on `ring`, and `reqwest` turns on
    // `aws-lc-rs`. With two providers, `ServerConfig::builder()` cannot pick a
    // process default and panics at run time. A named provider avoids that,
    // and it does not change the process default that other TLS clients read.
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let builder = ServerConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .expect("the ring provider supports rustls's default protocol versions");

    let builder = match client_ca {
        None => builder.with_no_client_auth(),
        Some(path) => {
            let mut roots = RootCertStore::empty();
            for certificate in read_certificates(path)? {
                roots
                    .add(certificate)
                    .map_err(|error| ServerSecurityError::InvalidClientCa {
                        path: path.to_owned(),
                        reason: error.to_string(),
                    })?;
            }
            let verifier = WebPkiClientVerifier::builder_with_provider(Arc::new(roots), provider);
            let verifier = if client_auth == ClientAuth::RequestClientCert {
                verifier.allow_unauthenticated()
            } else {
                verifier
            };
            let verifier =
                verifier
                    .build()
                    .map_err(|error| ServerSecurityError::InvalidClientCa {
                        path: path.to_owned(),
                        reason: error.to_string(),
                    })?;
            builder.with_client_cert_verifier(verifier)
        }
    };

    let mut config = builder.with_single_cert(chain, key).map_err(|source| {
        ServerSecurityError::InvalidServerCertificate {
            certificate: certificate_path.to_owned(),
            key: key_path.to_owned(),
            source,
        }
    })?;
    // The HTTP listeners serve HTTP/2 and HTTP/1.1 on one port, and a gRPC
    // client refuses a TLS connection that did not agree on `h2`.
    config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];

    Ok(Some(ServerTls {
        config: Arc::new(config),
        verifies_client_certificates: client_ca.is_some(),
        handshake_timeout: args.server_tls_handshake_timeout,
    }))
}
