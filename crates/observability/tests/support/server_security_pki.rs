//! Certificate authorities and leaf certificates, shared by the
//! `server_security` unit tests, the `server_security` and
//! `service_security` suites, and the TLS suites of //crates/metrics-service
//! and //crates/profiles.
//!
//! Each reaches this file with `#[path]`, so it names only external crates.

use rcgen::{
    BasicConstraints, CertificateParams, CertifiedIssuer, DnType, ExtendedKeyUsagePurpose, IsCa,
    KeyPair, KeyUsagePurpose,
};

/// A certificate and its private key, as PEM.
pub struct Pem {
    pub certificate: String,
    pub key: String,
}

/// A self-signed certificate authority named `common_name`.
pub fn authority(common_name: &str) -> CertifiedIssuer<'static, KeyPair> {
    let mut params = CertificateParams::new(Vec::<String>::new()).expect("valid parameters");
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    params
        .distinguished_name
        .push(DnType::CommonName, common_name);
    CertifiedIssuer::self_signed(params, KeyPair::generate().expect("a key")).expect("a CA")
}

/// The subject of a leaf certificate.
pub struct Leaf<'a> {
    pub common_name: &'a str,
    /// The subject alternative names.
    pub names: &'a [&'a str],
    pub usage: ExtendedKeyUsagePurpose,
}

impl Leaf<'_> {
    /// The server certificate every suite serves: `server`, valid for
    /// `localhost` and `127.0.0.1`.
    pub const LOCAL_SERVER: Leaf<'static> = Leaf {
        common_name: "server",
        names: &["localhost", "127.0.0.1"],
        usage: ExtendedKeyUsagePurpose::ServerAuth,
    };

    /// This leaf, with a fresh key, signed by `authority`.
    pub fn signed_by(&self, authority: &CertifiedIssuer<'static, KeyPair>) -> Pem {
        let mut params = CertificateParams::new(
            self.names
                .iter()
                .map(|name| (*name).to_owned())
                .collect::<Vec<_>>(),
        )
        .expect("valid parameters");
        params
            .distinguished_name
            .push(DnType::CommonName, self.common_name);
        params.extended_key_usages = vec![self.usage.clone()];
        let key = KeyPair::generate().expect("a key");
        let certificate = params.signed_by(&key, authority).expect("a signed leaf");
        Pem {
            certificate: certificate.pem(),
            key: key.serialize_pem(),
        }
    }
}
