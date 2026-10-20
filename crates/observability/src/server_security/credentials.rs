use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    sync::Arc,
};

use krabka_blockstore::TenantId;
use serde::Deserialize;
use subtle::ConditionallySelectable;

use super::{
    AuthFailureReason, ClientIdentity, CredentialsError, TenantGrant, token_digest::TokenDigest,
};

struct TokenOwner {
    digest: TokenDigest,
    principal_index: u64,
}

/// A validated credentials file.
pub struct Credentials {
    principals: Vec<ConfiguredPrincipal>,
    /// Every token digest, with one more than the index of its principal.
    /// Zero is never an owner, so a constant-time scan can start from it.
    token_digests: Vec<TokenOwner>,
    /// Every client certificate identity, with the index of its principal.
    client_certificates: BTreeMap<String, usize>,
}

impl fmt::Debug for Credentials {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Credentials")
            .field("principal_count", &self.principals.len())
            .field("token_count", &self.token_digests.len())
            .field("client_certificate_count", &self.client_certificates.len())
            .finish()
    }
}

impl Credentials {
    /// The tenant entry that grants every tenant.
    const EVERY_TENANT: &str = "*";

    /// Parses and validates a credentials file.
    pub fn from_yaml(yaml: &[u8]) -> Result<Self, CredentialsError> {
        let file: CredentialsFile = serde_yaml::from_slice(yaml)?;
        if file.principals.is_empty() {
            return Err(CredentialsError::NoPrincipals);
        }
        let mut credentials = Self {
            principals: Vec::with_capacity(file.principals.len()),
            token_digests: Vec::new(),
            client_certificates: BTreeMap::new(),
        };
        for principal_entry in file.principals.into_iter().enumerate() {
            let index = principal_entry.0;
            let entry = principal_entry.1;
            if entry.name.is_empty() {
                return Err(CredentialsError::EmptyName { index });
            }
            if credentials
                .principals
                .iter()
                .any(|principal| *principal.name == *entry.name)
            {
                return Err(CredentialsError::DuplicateName { name: entry.name });
            }
            if entry.token_sha256.is_empty() && entry.client_certificates.is_empty() {
                return Err(CredentialsError::NoCredential { name: entry.name });
            }
            let tenants = Self::parse_tenant_grant(&entry.name, &entry.tenants)?;
            let owner = u64::try_from(index + 1).expect("a principal count fits in u64");
            for token_entry in entry.token_sha256.iter().enumerate() {
                let position = token_entry.0;
                let hex = token_entry.1;
                let digest = TokenDigest::parse(hex).ok_or_else(|| {
                    CredentialsError::InvalidTokenDigest {
                        name: entry.name.clone(),
                        position,
                    }
                })?;
                if let Some(owner) = credentials
                    .token_digests
                    .iter()
                    .find(|known| bool::from(known.digest.ct_eq(&digest)))
                {
                    let first = usize::try_from(owner.principal_index - 1)
                        .expect("an owner index fits in usize");
                    let first = credentials.principals.get(first).map_or_else(
                        || entry.name.clone(),
                        |principal| principal.name.to_string(),
                    );
                    return Err(CredentialsError::DuplicateTokenDigest {
                        first,
                        second: entry.name,
                    });
                }
                credentials.token_digests.push(TokenOwner {
                    digest,
                    principal_index: owner,
                });
            }
            for identity in &entry.client_certificates {
                if identity.is_empty() {
                    return Err(CredentialsError::EmptyClientCertificate { name: entry.name });
                }
                if let Some(first) = credentials.client_certificates.get(identity) {
                    let first = credentials.principals.get(*first).map_or_else(
                        || entry.name.clone(),
                        |principal| principal.name.to_string(),
                    );
                    return Err(CredentialsError::DuplicateClientCertificate {
                        identity: identity.clone(),
                        first,
                        second: entry.name,
                    });
                }
                credentials
                    .client_certificates
                    .insert(identity.clone(), index);
            }
            credentials.principals.push(ConfiguredPrincipal {
                name: Arc::from(entry.name),
                tenants,
                admin: entry.admin,
            });
        }
        Ok(credentials)
    }

    /// Whether any principal names a client certificate identity.
    pub fn maps_client_certificates(&self) -> bool {
        !self.client_certificates.is_empty()
    }

    /// The principal that owns `token`.
    ///
    /// The scan compares the token's digest with every configured digest in
    /// constant time, and it does not stop at a match. Its run time depends
    /// on the number of digests, and not on which digest matched or on how
    /// many leading bytes agreed.
    pub fn principal_for_token(&self, token: &[u8]) -> Option<&ConfiguredPrincipal> {
        let presented = TokenDigest::of_token(token);
        let mut owner = 0_u64;
        for candidate in &self.token_digests {
            owner.conditional_assign(
                &candidate.principal_index,
                candidate.digest.ct_eq(&presented),
            );
        }
        let index = usize::try_from(owner.checked_sub(1)?).ok()?;
        self.principals.get(index)
    }

    /// The principal whose name is `username` and that owns `token`.
    pub fn principal_for_basic(
        &self,
        username: &str,
        token: &[u8],
    ) -> Option<&ConfiguredPrincipal> {
        self.principal_for_token(token)
            .filter(|principal| *principal.name == *username)
    }

    /// The one principal that a verified client certificate names.
    pub fn principal_for_client_identity(
        &self,
        identity: &ClientIdentity,
    ) -> Result<&ConfiguredPrincipal, AuthFailureReason> {
        let owners: BTreeSet<usize> = identity
            .names()
            .filter_map(|name| self.client_certificates.get(name).copied())
            .collect();
        let mut owners = owners.into_iter();
        let Some(index) = owners.next() else {
            return Err(AuthFailureReason::UnknownCredential);
        };
        if owners.next().is_some() {
            return Err(AuthFailureReason::AmbiguousClientCertificate);
        }
        self.principals
            .get(index)
            .ok_or(AuthFailureReason::UnknownCredential)
    }

    fn parse_tenant_grant(name: &str, tenants: &[String]) -> Result<TenantGrant, CredentialsError> {
        if tenants.iter().any(|tenant| tenant == Self::EVERY_TENANT) {
            return if tenants.len() == 1 {
                Ok(TenantGrant::All)
            } else {
                Err(CredentialsError::WildcardWithOtherTenants {
                    name: name.to_owned(),
                })
            };
        }
        tenants
            .iter()
            .map(|tenant| {
                TenantId::new(tenant.as_str()).map_err(|source| CredentialsError::InvalidTenant {
                    name: name.to_owned(),
                    tenant: tenant.clone(),
                    source,
                })
            })
            .collect::<Result<BTreeSet<_>, _>>()
            .map(|tenants| TenantGrant::Only(Arc::new(tenants)))
    }
}

/// One principal from the credentials file, after validation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfiguredPrincipal {
    pub name: Arc<str>,
    pub tenants: TenantGrant,
    pub admin: bool,
}

/// The YAML structure of the credentials file.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CredentialsFile {
    pub principals: Vec<PrincipalEntry>,
}

/// One entry of `principals`, before validation.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PrincipalEntry {
    pub name: String,
    #[serde(default)]
    pub token_sha256: Vec<String>,
    #[serde(default)]
    pub client_certificates: Vec<String>,
    pub tenants: Vec<String>,
    #[serde(default)]
    pub admin: bool,
}
