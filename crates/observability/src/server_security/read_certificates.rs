use std::path::Path;

use rustls_pki_types::{CertificateDer, pem::PemObject};

use super::{ServerSecurityError, read_file::read_file};

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
