use std::path::Path;

use rustls_pki_types::{PrivateKeyDer, pem::PemObject};

use super::{ServerSecurityError, read_file::read_file};

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
