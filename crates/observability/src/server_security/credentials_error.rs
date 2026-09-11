use krabka_blockstore::TenantIdError;

/// Why a credentials file was rejected.
///
/// No message holds a token digest.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum CredentialsError {
    /// The file is not YAML, or it does not have the credentials-file shape.
    /// An unknown key is also this error.
    #[error("{0}")]
    Yaml(#[from] serde_yaml::Error),

    #[error("the file lists no principals")]
    NoPrincipals,

    #[error("principal {index} has an empty name")]
    EmptyName { index: usize },

    #[error("principal {name:?} appears more than once")]
    DuplicateName { name: String },

    #[error("principal {name:?} has no token_sha256 entry and no client_certificates entry")]
    NoCredential { name: String },

    #[error("principal {name:?}: tenant {tenant:?} is not a valid tenant id: {source}")]
    InvalidTenant {
        name: String,
        tenant: String,
        source: TenantIdError,
    },

    #[error("principal {name:?}: `*` must be the only entry in tenants")]
    WildcardWithOtherTenants { name: String },

    /// `position` counts from zero within the principal's `token_sha256` list.
    #[error(
        "principal {name:?}: token_sha256 entry {position} is not exactly 64 lowercase hexadecimal characters"
    )]
    InvalidTokenDigest { name: String, position: usize },

    #[error("a token_sha256 digest belongs to both {first:?} and {second:?}")]
    DuplicateTokenDigest { first: String, second: String },

    #[error("principal {name:?}: client_certificates holds an empty identity")]
    EmptyClientCertificate { name: String },

    #[error("client certificate identity {identity:?} belongs to both {first:?} and {second:?}")]
    DuplicateClientCertificate {
        identity: String,
        first: String,
        second: String,
    },
}
