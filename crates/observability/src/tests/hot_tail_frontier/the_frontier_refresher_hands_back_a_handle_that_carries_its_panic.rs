use assert2::assert;
use krabka_units::{convert::TimeExt as _, millis};
use tokio_util::sync::CancellationToken;

use super::*;
use crate::compactor::runtime::spawn_compaction_frontier_refresher;

/// An object store that dies when the frontier is read.
///
/// A refresh that returns an error is not the case under test: the loop logs
/// that and carries on. What ends the task without a word is a panic, and a
/// store that panics on `get` is the shortest way to one.
#[derive(Debug)]
struct PanicOnGetStore(object_store::memory::InMemory);

impl std::fmt::Display for PanicOnGetStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PanicOnGetStore")
    }
}

#[async_trait::async_trait]
impl ObjectStore for PanicOnGetStore {
    async fn put_opts(
        &self,
        location: &object_store::path::Path,
        payload: object_store::PutPayload,
        opts: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        self.0.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &object_store::path::Path,
        opts: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.0.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        _location: &object_store::path::Path,
        _options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        panic!("the frontier object could not be read");
    }

    fn delete_stream(
        &self,
        locations: futures_util::stream::BoxStream<
            'static,
            object_store::Result<object_store::path::Path>,
        >,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::path::Path>>
    {
        self.0.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
    {
        self.0.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> object_store::Result<object_store::ListResult> {
        self.0.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &object_store::path::Path,
        to: &object_store::path::Path,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.0.copy_opts(from, to, options).await
    }
}

/// The refresher's death reaches its caller.
///
/// The task honours cancellation, so shutdown always looked fine, and that is
/// what hid this: a panic inside the loop ended the refresher, the querier
/// carried on answering from the frontier it last read, and nothing anywhere
/// said the frontier had stopped moving. The handle is what makes that
/// observable, so the function has to give it back and the caller has to be
/// able to join it.
#[tokio::test]
async fn the_frontier_refresher_hands_back_a_handle_that_carries_its_panic() {
    let token = CancellationToken::new();
    let handle = spawn_compaction_frontier_refresher(
        Arc::new(PanicOnGetStore(object_store::memory::InMemory::new())),
        ObjectPath::default(),
        SharedCompactionFrontier::default(),
        BufferedLogHotTail::default(),
        token.clone(),
        millis(1),
    );

    let joined = tokio::time::timeout(millis(2_000).to_std(), handle)
        .await
        .expect("a panicking refresher ends, so its handle resolves");

    let error = joined.expect_err("the refresher panicked, so the join must not succeed");
    assert!(error.is_panic());
    // Cancellation was never asked for: the task ended on its own, which is
    // exactly what a supervisor must be able to tell apart from shutdown.
    assert!(!token.is_cancelled());
}
