use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use krabka_blockstore::TenantId;
use subtle::ConditionallySelectable;

use super::{
    AuthFailureReason, ClientIdentity, CredentialsError, TenantGrant,
    configured_principal::ConfiguredPrincipal, credentials_file::CredentialsFile,
    token_digest::TokenDigest,
};

/// A validated credentials file.
#[derive(Debug)]
pub struct Credentials {
    principals: Vec<ConfiguredPrincipal>,
    /// Every token digest, with one more than the index of its principal.
    /// Zero is never an owner, so a constant-time scan can start from it.
    token_digests: Vec<(TokenDigest, u64)>,
    /// Every client certificate identity, with the index of its principal.
    client_certificates: BTreeMap<String, usize>,
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
        for (index, entry) in file.principals.into_iter().enumerate() {
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
            for (position, hex) in entry.token_sha256.iter().enumerate() {
                let digest = TokenDigest::parse(hex).ok_or_else(|| {
                    CredentialsError::InvalidTokenDigest {
                        name: entry.name.clone(),
                        position,
                    }
                })?;
                if let Some((_, first)) = credentials
                    .token_digests
                    .iter()
                    .find(|(known, _)| bool::from(known.ct_eq(&digest)))
                {
                    let first = usize::try_from(*first - 1).expect("an owner index fits in usize");
                    let first = credentials.principals.get(first).map_or_else(
                        || entry.name.clone(),
                        |principal| principal.name.to_string(),
                    );
                    return Err(CredentialsError::DuplicateTokenDigest {
                        first,
                        second: entry.name,
                    });
                }
                credentials.token_digests.push((digest, owner));
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
        for (digest, candidate) in &self.token_digests {
            owner.conditional_assign(candidate, digest.ct_eq(&presented));
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
        match (owners.next(), owners.next()) {
            (Some(index), None) => self
                .principals
                .get(index)
                .ok_or(AuthFailureReason::UnknownCredential),
            (Some(_), Some(_)) => Err(AuthFailureReason::AmbiguousClientCertificate),
            (None, _) => Err(AuthFailureReason::UnknownCredential),
        }
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
