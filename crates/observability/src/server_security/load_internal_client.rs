use axum::http::HeaderValue;

use super::{InternalClient, ServerSecurityArgs, ServerSecurityError, read_file::read_file};

/// Checks the `--internal-client-*` flags together, and loads the files they name.
pub fn load_internal_client(
    args: &ServerSecurityArgs,
) -> Result<InternalClient, ServerSecurityError> {
    let authorization = match args.internal_client_token_path.as_deref() {
        None => None,
        Some(path) => {
            let invalid = || ServerSecurityError::InvalidInternalClientToken {
                path: path.to_owned(),
            };
            let token = read_file(path)?;
            let token = token.trim_ascii();
            if token.is_empty() {
                return Err(invalid());
            }
            let mut value =
                HeaderValue::from_bytes(&[b"Bearer ", token].concat()).map_err(|_| invalid())?;
            value.set_sensitive(true);
            Some(value)
        }
    };

    let certificate = args.internal_client_tls_cert_path.as_deref();
    let key = args.internal_client_tls_key_path.as_deref();
    let identity = if let Some(certificate) = certificate {
        let Some(key) = key else {
            return Err(ServerSecurityError::InternalClientCertificateWithoutKey);
        };
        let pem = [read_file(certificate)?, read_file(key)?].concat();
        let identity = reqwest::Identity::from_pem(&pem).map_err(|source| {
            ServerSecurityError::InvalidInternalClientIdentity {
                certificate: certificate.to_owned(),
                key: key.to_owned(),
                source,
            }
        })?;
        Some(identity)
    } else if key.is_some() {
        return Err(ServerSecurityError::InternalClientKeyWithoutCertificate);
    } else {
        None
    };

    let trusted_roots = match args.internal_client_tls_ca_path.as_deref() {
        None => Vec::new(),
        Some(path) => {
            let roots =
                reqwest::Certificate::from_pem_bundle(&read_file(path)?).map_err(|source| {
                    ServerSecurityError::InvalidInternalClientCa {
                        path: path.to_owned(),
                        source,
                    }
                })?;
            if roots.is_empty() {
                return Err(ServerSecurityError::NoCertificate {
                    path: path.to_owned(),
                });
            }
            roots
        }
    };

    Ok(InternalClient::new(authorization, identity, trusted_roots))
}
