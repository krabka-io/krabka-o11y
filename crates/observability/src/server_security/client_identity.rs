use x509_parser::{
    certificate::X509Certificate, error::X509Error, extensions::GeneralName, prelude::FromDer,
};

/// The names in a client certificate that the TLS handshake verified.
///
/// A [`PeerAddr`](super::PeerAddr) holds one of these only when rustls
/// verified the certificate against the configured client CA.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientIdentity {
    /// The first common name (CN) in the certificate subject.
    pub common_name: Option<String>,
    /// The DNS names in the subject alternative name extension.
    pub dns_names: Vec<String>,
    /// The URIs in the subject alternative name extension.
    pub uris: Vec<String>,
}

impl ClientIdentity {
    /// Reads the identity out of a DER-encoded certificate.
    ///
    /// # Errors
    ///
    /// Returns an [`X509Error`] when the certificate or its subject
    /// alternative name extension does not parse.
    pub fn from_der(der: &[u8]) -> Result<Self, X509Error> {
        let (_, certificate) = X509Certificate::from_der(der).map_err(|error| match error {
            x509_parser::nom::Err::Error(error) | x509_parser::nom::Err::Failure(error) => error,
            x509_parser::nom::Err::Incomplete(_) => X509Error::InvalidCertificate,
        })?;
        let common_name = certificate
            .subject()
            .iter_common_name()
            .find_map(|attribute| attribute.as_str().ok())
            .map(str::to_owned);
        let mut dns_names = Vec::new();
        let mut uris = Vec::new();
        if let Some(extension) = certificate.subject_alternative_name()? {
            for name in &extension.value.general_names {
                match name {
                    GeneralName::DNSName(dns) => dns_names.push((*dns).to_owned()),
                    GeneralName::URI(uri) => uris.push((*uri).to_owned()),
                    _ => {}
                }
            }
        }
        Ok(Self {
            common_name,
            dns_names,
            uris,
        })
    }

    /// Every name in the identity: the common name, then the DNS names, then the URIs.
    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.common_name
            .iter()
            .chain(&self.dns_names)
            .chain(&self.uris)
            .map(String::as_str)
    }
}
