//! Bounded retry for transient object-store failures on the write path.
//!
//! An object store fails transiently as a matter of routine: a 503 while a
//! partition moves, a reset connection, a request that times out. A write loop
//! that treats every one of those as fatal does not lose data -- its WAL
//! offsets are still uncommitted -- but it exits, and under a restart policy it
//! comes back cold, re-reads the same window and fails the same way. A
//! persistent fault then becomes an unbounded retry at *process* granularity,
//! which is the expensive way to do what this module does cheaply.
//!
//! Three pieces: [`is_transient_object_store_error`] decides what is worth
//! another attempt, [`ObjectStoreRetryPolicy`] says how many and how far apart,
//! and [`retry_object_store`] runs the loop. [`RetryingObjectStore`] applies
//! all three to a whole store.

use std::{error::Error, future::Future, num::NonZeroU32, sync::Arc, time::Duration};

use async_trait::async_trait;
use futures::stream::BoxStream;
use object_store::{
    CopyOptions, Error as ObjectStoreError, GetOptions, GetResult, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, path::Path,
};
use tokio::time::sleep;
use tracing::warn;

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use assert2::check;
    use futures::StreamExt as _;
    use object_store::{
        ObjectStore, ObjectStoreExt as _, PutPayload, memory::InMemory, path::Path,
    };
    use parquet::errors::ParquetError;

    use super::*;

    /// An object store whose first `failures` calls to `put_opts` fail with
    /// `error`, and which counts every attempt. Everything else delegates to
    /// an in-memory store, so what a retry actually wrote can be read back.
    #[derive(Debug)]
    struct FlakyObjectStore {
        inner: Arc<InMemory>,
        remaining_failures: AtomicUsize,
        attempts: AtomicUsize,
        error: fn() -> ObjectStoreError,
    }

    impl FlakyObjectStore {
        fn new(failures: usize, error: fn() -> ObjectStoreError) -> Self {
            Self {
                inner: Arc::new(InMemory::new()),
                remaining_failures: AtomicUsize::new(failures),
                attempts: AtomicUsize::new(0),
                error,
            }
        }

        fn attempts(&self) -> usize {
            self.attempts.load(Ordering::SeqCst)
        }

        async fn keys(&self) -> Vec<String> {
            let mut keys: Vec<String> = self
                .inner
                .list(None)
                .map(|meta| {
                    meta.expect("listing an in-memory store")
                        .location
                        .to_string()
                })
                .collect()
                .await;
            keys.sort();
            keys
        }
    }

    impl std::fmt::Display for FlakyObjectStore {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("FlakyObjectStore")
        }
    }

    #[async_trait]
    impl ObjectStore for FlakyObjectStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            options: PutOptions,
        ) -> object_store::Result<PutResult> {
            self.attempts.fetch_add(1, Ordering::SeqCst);
            if self
                .remaining_failures
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |left| {
                    (left > 0).then(|| left - 1)
                })
                .is_ok()
            {
                return Err((self.error)());
            }
            self.inner.put_opts(location, payload, options).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            options: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, options).await
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            self.inner.get_opts(location, options).await
        }

        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, object_store::Result<Path>>,
        ) -> BoxStream<'static, object_store::Result<Path>> {
            self.inner.delete_stream(locations)
        }
    }

    fn timeout() -> ObjectStoreError {
        ObjectStoreError::Generic {
            store: "S3",
            source: "operation timed out".into(),
        }
    }

    fn forbidden() -> ObjectStoreError {
        ObjectStoreError::PermissionDenied {
            path: "blocks/a.parquet".to_string(),
            source: "403".into(),
        }
    }

    /// The classifier is the whole design decision, so it gets a table: one
    /// row per way an object store says no, and whether waiting could change
    /// the answer.
    #[test]
    fn only_an_indeterminate_backend_failure_is_worth_another_attempt() {
        let cases: Vec<(&str, ObjectStoreError, bool)> = vec![
            ("a 5xx, a timeout or a reset connection", timeout(), true),
            ("a 403 on a credential that was revoked", forbidden(), false),
            (
                "a 401 that no amount of waiting authenticates",
                ObjectStoreError::Unauthenticated {
                    path: "blocks/a.parquet".to_string(),
                    source: "401".into(),
                },
                false,
            ),
            (
                "a 404, which is this object's answer and not the store's",
                ObjectStoreError::NotFound {
                    path: "blocks/a.parquet".to_string(),
                    source: "404".into(),
                },
                false,
            ),
            (
                "a failed precondition",
                ObjectStoreError::Precondition {
                    path: "blocks/a.parquet".to_string(),
                    source: "412".into(),
                },
                false,
            ),
            (
                "an object the store refuses to overwrite",
                ObjectStoreError::AlreadyExists {
                    path: "blocks/a.parquet".to_string(),
                    source: "409".into(),
                },
                false,
            ),
            (
                "a configuration key the backend does not have",
                ObjectStoreError::UnknownConfigurationKey {
                    store: "S3",
                    key: "nonsense".to_string(),
                },
                false,
            ),
        ];
        for (what, error, want) in cases {
            check!(is_transient_object_store_error(&error) == want, "{what}");
        }
    }

    /// A block write fails as a Parquet error wrapping an I/O error wrapping
    /// the backend error. The classifier has to reach through both, or every
    /// block write would look permanent.
    #[test]
    fn a_backend_failure_is_found_through_the_parquet_and_io_layers_that_wrap_it() {
        let wrapped = |inner: ObjectStoreError| {
            crate::BlockStoreError::from(ParquetError::External(Box::new(std::io::Error::from(
                inner,
            ))))
        };

        check!(transient_object_store_error(&wrapped(timeout())).is_some());
        check!(transient_object_store_error(&wrapped(forbidden())).is_none());
        // A block that is simply not a Parquet block has no backend error
        // under it at all, and must not be retried either.
        check!(
            transient_object_store_error(&crate::BlockStoreError::from(ParquetError::General(
                "corrupt footer".to_string()
            )))
            .is_none()
        );
    }

    #[tokio::test]
    async fn a_transient_failure_is_retried_and_a_permanent_one_is_not() {
        // Two failures then success, against a budget of four: the operation
        // succeeds, having been attempted three times.
        let attempts = Arc::new(AtomicUsize::new(0));
        let counted = Arc::clone(&attempts);
        let result: Result<(), ObjectStoreError> =
            retry_object_store(ObjectStoreRetryPolicy::immediate(4), "test", || {
                let attempts = Arc::clone(&counted);
                async move {
                    if attempts.fetch_add(1, Ordering::SeqCst) < 2 {
                        Err(timeout())
                    } else {
                        Ok(())
                    }
                }
            })
            .await;
        check!(result.is_ok());
        check!(attempts.load(Ordering::SeqCst) == 3);

        // A permanent failure is reported on the first attempt, not the
        // fourth: the budget is for transient faults, and spending it here
        // only delays the report.
        let attempts = Arc::new(AtomicUsize::new(0));
        let counted = Arc::clone(&attempts);
        let result: Result<(), ObjectStoreError> =
            retry_object_store(ObjectStoreRetryPolicy::immediate(4), "test", || {
                let attempts = Arc::clone(&counted);
                async move {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    Err(forbidden())
                }
            })
            .await;
        check!(result.is_err());
        check!(attempts.load(Ordering::SeqCst) == 1);

        // And the budget is a budget: a fault that never clears is reported
        // after `max_attempts`, not retried forever.
        let attempts = Arc::new(AtomicUsize::new(0));
        let counted = Arc::clone(&attempts);
        let result: Result<(), ObjectStoreError> =
            retry_object_store(ObjectStoreRetryPolicy::immediate(4), "test", || {
                let attempts = Arc::clone(&counted);
                async move {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    Err(timeout())
                }
            })
            .await;
        check!(result.is_err());
        check!(attempts.load(Ordering::SeqCst) == 4);
    }

    #[tokio::test]
    async fn a_wrapped_store_puts_the_same_object_once_the_backend_recovers() {
        let flaky = Arc::new(FlakyObjectStore::new(2, timeout));
        let store = RetryingObjectStore::wrap(
            Arc::clone(&flaky) as Arc<dyn ObjectStore>,
            ObjectStoreRetryPolicy::immediate(4),
        );

        store
            .put(
                &Path::from("index/manifest"),
                PutPayload::from_static(b"v1"),
            )
            .await
            .expect("the third attempt succeeds");

        check!(flaky.attempts() == 3);
        // One key in, one object out: a retried put overwrites, it does not
        // add a second object.
        let listed = flaky.keys().await;
        check!(listed == vec!["index/manifest".to_string()]);
    }
}

mod is_transient_object_store_error;
mod object_store_retry_policy;
mod retry_object_store;
mod retrying_object_store;
mod transient_object_store_error;

pub use is_transient_object_store_error::is_transient_object_store_error;
pub use object_store_retry_policy::ObjectStoreRetryPolicy;
pub use retry_object_store::retry_object_store;
pub use retrying_object_store::RetryingObjectStore;
pub use transient_object_store_error::transient_object_store_error;
