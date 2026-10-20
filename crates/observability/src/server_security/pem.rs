use std::path::Path;

use rustls_pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject};

use super::{ServerSecurityError, read_file};

/// Reads every PEM certificate in `path`, in file order.
pub fn read_certificates(path: &Path) -> Result<Vec<CertificateDer<'static>>, ServerSecurityError> {
    let pem = read_file(path)?;
    let certificates = CertificateDer::pem_slice_iter(&pem)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| ServerSecurityError::InvalidPem {
            path: path.to_owned(),
            reason: error.to_string(),
        })?;
    if certificates.is_empty() {
        return Err(ServerSecurityError::NoCertificate {
            path: path.to_owned(),
        });
    }
    Ok(certificates)
}

/// Reads the first PEM private key in `path`: PKCS#8, PKCS#1 or SEC1.
pub fn read_private_key(path: &Path) -> Result<PrivateKeyDer<'static>, ServerSecurityError> {
    let pem = read_file(path)?;
    PrivateKeyDer::from_pem_slice(&pem).map_err(|error| match error {
        rustls_pki_types::pem::Error::NoItemsFound => ServerSecurityError::NoPrivateKey {
            path: path.to_owned(),
        },
        error => ServerSecurityError::InvalidPem {
            path: path.to_owned(),
            reason: error.to_string(),
        },
    })
}
