use std::sync::Arc;

use rustls::{RootCertStore, ServerConfig, server::WebPkiClientVerifier};

use super::{
    ClientAuth, ServerSecurityArgs, ServerSecurityError, read_certificates::read_certificates,
    read_private_key::read_private_key, server_tls::ServerTls,
};

struct ServerCertificatePaths<'a> {
    certificate: &'a std::path::Path,
    key: &'a std::path::Path,
}

/// Checks the `--server-tls-*` flags together, and builds the TLS configuration they describe.
///
/// Returns `None` when no TLS flag is set.
pub fn load_server_tls(
    args: &ServerSecurityArgs,
) -> Result<Option<ServerTls>, ServerSecurityError> {
    let client_auth = args.server_tls_client_auth;
    let client_ca = args.server_tls_client_ca_path.as_deref();
    let certificate_paths = if let Some(certificate) = args.server_tls_cert_path.as_deref() {
        let Some(key) = args.server_tls_key_path.as_deref() else {
            return Err(ServerSecurityError::CertificateWithoutKey);
        };
        ServerCertificatePaths { certificate, key }
    } else if args.server_tls_key_path.is_some() {
        return Err(ServerSecurityError::KeyWithoutCertificate);
    } else if client_ca.is_some() {
        return Err(ServerSecurityError::ClientCaWithoutCertificate);
    } else if client_auth != ClientAuth::NoClientCert {
        return Err(ServerSecurityError::ClientAuthWithoutCertificate { client_auth });
    } else {
        return Ok(None);
    };
    let client_ca = if client_auth == ClientAuth::NoClientCert {
        if client_ca.is_some() {
            return Err(ServerSecurityError::ClientCaWithoutClientAuth);
        }
        None
    } else if let Some(path) = client_ca {
        Some(path)
    } else {
        return Err(ServerSecurityError::ClientAuthWithoutClientCa { client_auth });
    };

    let chain = read_certificates(certificate_paths.certificate)?;
    let key = read_private_key(certificate_paths.key)?;

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
            certificate: certificate_paths.certificate.to_owned(),
            key: certificate_paths.key.to_owned(),
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
