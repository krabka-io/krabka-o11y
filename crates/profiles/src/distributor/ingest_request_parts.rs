use axum::{
    extract::{FromRequestParts, rejection::ExtensionRejection},
    http::request::Parts,
};

use super::{Arc, DistributorState, Extension, HeaderMap, Principal};

/// The request parts every distributor ingest door reads: the shared
/// distributor state, the authenticated caller, and the request headers.
///
/// It extracts them in that order, so a missing extension is rejected exactly
/// as the `Extension` extractor rejects it.
pub(crate) struct IngestRequestParts {
    pub(crate) state: Arc<DistributorState>,
    pub(crate) principal: Principal,
    pub(crate) headers: HeaderMap,
}

impl<S: Send + Sync> FromRequestParts<S> for IngestRequestParts {
    type Rejection = ExtensionRejection;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let Extension(distributor) =
            Extension::<Arc<DistributorState>>::from_request_parts(parts, state).await?;
        let Extension(principal) = Extension::<Principal>::from_request_parts(parts, state).await?;
        Ok(Self {
            state: distributor,
            principal,
            headers: parts.headers.clone(),
        })
    }
}
