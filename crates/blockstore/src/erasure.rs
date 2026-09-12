use std::sync::Arc;

use futures::TryStreamExt as _;
use object_store::{ObjectStore, ObjectStoreExt as _, PutPayload, path::Path};
use serde::{Deserialize, Serialize};

use crate::{LabelMatcher, Result, escape_object_path_segment};

/// One persisted request to remove selected rows in a closed time range.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ErasureRequest {
    /// Stable identifier derived from the request contents.
    pub id: String,
    /// Tenant that owns the selected data.
    pub tenant: String,
    /// The selector supplied by the operator, for audit and list responses.
    pub selector: String,
    /// Prometheus matcher alternatives resolved against each block during compaction.
    pub matcher_sets: Vec<Vec<LabelMatcher>>,
    /// Inclusive Unix nanosecond boundary.
    pub start_ns: i64,
    /// Inclusive Unix nanosecond boundary.
    pub end_ns: i64,
    /// Unix nanosecond time at which the request was accepted.
    pub created_at_ns: i64,
    /// Whether the operator asked compaction to retire this request after a clean pass.
    #[serde(default)]
    pub clean_requested: bool,
}

impl ErasureRequest {
    /// Creates a request and derives its stable object ID from its contents.
    ///
    /// # Panics
    /// Panics if the string-and-matcher identity tuple cannot be serialized.
    #[must_use]
    pub fn new(
        tenant: impl Into<String>,
        selector: impl Into<String>,
        matcher_sets: Vec<Vec<LabelMatcher>>,
        start_ns: i64,
        end_ns: i64,
        created_at_ns: i64,
    ) -> Self {
        let tenant = tenant.into();
        let selector = selector.into();
        let identity = serde_json::to_vec(&(
            &tenant,
            &selector,
            &matcher_sets,
            start_ns,
            end_ns,
            created_at_ns,
        ))
        .expect("erasure request identity serialises");
        let id = format!("delete-{:016x}", xxhash_rust::xxh3::xxh3_64(&identity));
        Self {
            id,
            tenant,
            selector,
            matcher_sets,
            start_ns,
            end_ns,
            created_at_ns,
            clean_requested: false,
        }
    }

    /// Returns true when the request overlaps the closed range.
    #[must_use]
    pub const fn overlaps(&self, min_ns: i64, max_ns: i64) -> bool {
        self.start_ns <= max_ns && self.end_ns >= min_ns
    }
}

/// Stores an erasure request as one independently removable object.
///
/// # Errors
/// Returns an error when serialization or object-store I/O fails.
pub async fn put_erasure_request(
    store: &Arc<dyn ObjectStore>,
    prefix: &str,
    request: &ErasureRequest,
) -> Result<()> {
    let key = erasure_request_key(prefix, &request.tenant, &request.id);
    store
        .put(
            &Path::from(key),
            PutPayload::from(serde_json::to_vec_pretty(request)?),
        )
        .await?;
    Ok(())
}

/// Lists active metric erasure requests, sorted by object ID.
///
/// # Errors
/// Returns an error when object-store I/O fails or a request is malformed.
pub async fn list_erasure_requests(
    store: &Arc<dyn ObjectStore>,
    prefix: &str,
) -> Result<Vec<ErasureRequest>> {
    let path = Path::from(prefix.trim_end_matches('/'));
    let mut objects = store.list(Some(&path)).try_collect::<Vec<_>>().await?;
    objects.sort_by(|left, right| left.location.cmp(&right.location));
    let mut requests = Vec::with_capacity(objects.len());
    for object in objects {
        let bytes = store.get(&object.location).await?.bytes().await?;
        requests.push(serde_json::from_slice(&bytes)?);
    }
    Ok(requests)
}

/// Removes a materialised or cancelled request.
///
/// # Errors
/// Returns an error when object-store deletion fails.
pub async fn delete_erasure_request(
    store: &Arc<dyn ObjectStore>,
    prefix: &str,
    request: &ErasureRequest,
) -> Result<()> {
    store
        .delete(&Path::from(erasure_request_key(
            prefix,
            &request.tenant,
            &request.id,
        )))
        .await?;
    Ok(())
}

fn erasure_request_key(prefix: &str, tenant: &str, id: &str) -> String {
    format!(
        "{}/{}/{}.json",
        prefix.trim_end_matches('/'),
        escape_object_path_segment(tenant),
        escape_object_path_segment(id)
    )
}

/// The object-store prefix for Prometheus metric erasure requests.
///
/// Tempo and Pyroscope define no equivalent deletion API, so Krabka does not
/// invent incompatible trace or profile endpoints.
pub const ERASURE_REQUEST_PREFIX: &str = "metric-erasure-requests";

#[cfg(test)]
mod tests {
    use assert2::check;
    use object_store::memory::InMemory;

    use super::*;

    #[tokio::test]
    async fn requests_round_trip_as_independent_objects() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let request = ErasureRequest::new(
            "tenant/a",
            "{user=\"7\"}",
            vec![vec![LabelMatcher::new("user", crate::MatchOp::Eq, "7")]],
            10,
            20,
            30,
        );

        put_erasure_request(&store, ERASURE_REQUEST_PREFIX, &request)
            .await
            .unwrap();
        check!(
            list_erasure_requests(&store, ERASURE_REQUEST_PREFIX)
                .await
                .unwrap()
                == vec![request.clone()]
        );
        check!(request.overlaps(20, 40));
        check!(!request.overlaps(21, 40));

        delete_erasure_request(&store, ERASURE_REQUEST_PREFIX, &request)
            .await
            .unwrap();
        check!(
            list_erasure_requests(&store, ERASURE_REQUEST_PREFIX)
                .await
                .unwrap()
                .is_empty()
        );
    }
}
