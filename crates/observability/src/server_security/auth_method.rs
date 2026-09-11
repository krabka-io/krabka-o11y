/// The kind of credential that authenticated a request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AuthMethod {
    /// An `Authorization: Bearer <token>` header.
    Bearer,
    /// An `Authorization: Basic <base64 name:token>` header.
    Basic,
    /// The verified client certificate of the TLS connection.
    ClientCertificate,
}
