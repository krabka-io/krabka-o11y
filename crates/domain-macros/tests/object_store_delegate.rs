use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use futures::{StreamExt as _, TryStreamExt as _, stream::BoxStream};
use krabka_domain_macros::delegate_object_store;
use object_store::{
    CopyOptions, GetOptions, GetRange, GetResult, ObjectMeta, ObjectStore, ObjectStoreExt as _,
    PutMode, PutOptions, memory::InMemory, path::Path,
};

#[derive(Debug)]
struct Store<T: ObjectStore> {
    inner: T,
    reads: AtomicUsize,
    listings: AtomicUsize,
}

impl<T: ObjectStore> std::fmt::Display for Store<T> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.inner, formatter)
    }
}

impl<T: ObjectStore> Store<T> {
    fn base(&self) -> &T {
        &self.inner
    }
}

#[delegate_object_store(self.base())]
#[async_trait]
impl<T> ObjectStore for Store<T>
where
    T: ObjectStore,
{
    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        // The tail after a let-chain must survive macro expansion.
        let calls = Some(&self.reads);
        if location.as_ref().starts_with("prefix/")
            && let Some(calls) = calls
        {
            calls.fetch_add(1, Ordering::SeqCst);
        }
        let result = self.inner.get_opts(location, options).await?;
        Ok(result)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.listings.fetch_add(1, Ordering::SeqCst);
        self.inner
            .list(prefix)
            .try_filter(|meta| futures::future::ready(!meta.location.as_ref().ends_with("/hidden")))
            .boxed()
    }
}

fn store() -> Store<InMemory> {
    Store {
        inner: InMemory::new(),
        reads: AtomicUsize::new(0),
        listings: AtomicUsize::new(0),
    }
}

#[tokio::test]
async fn preserves_options_and_dispatches_read_defaults_through_override() {
    let store = store();
    let location = Path::from("prefix/object");
    let bytes = Bytes::from_static(b"abcdef");
    let options = PutOptions {
        mode: PutMode::Create,
        ..Default::default()
    };
    let result = store
        .put_opts(&location, bytes.clone().into(), options.clone())
        .await
        .unwrap();
    assert2::assert!(matches!(
        store
            .put_opts(&location, bytes.clone().into(), options)
            .await,
        Err(object_store::Error::AlreadyExists { .. })
    ));
    let fetched = store
        .get_opts(
            &location,
            GetOptions {
                if_match: result.e_tag,
                range: Some(GetRange::Bounded(1..4)),
                ..Default::default()
            },
        )
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert2::assert!(fetched == Bytes::from_static(b"bcd"));
    assert2::assert!(matches!(
        store
            .get_opts(
                &location,
                GetOptions {
                    if_match: Some("wrong-etag".to_owned()),
                    ..Default::default()
                }
            )
            .await,
        Err(object_store::Error::Precondition { .. })
    ));
    assert2::assert!(store.head(&location).await.unwrap().size == 6);
    assert2::assert!(store.get(&location).await.unwrap().bytes().await.unwrap() == bytes);
    let ranges = [0..2, 4..6];
    assert2::assert!(
        store.get_ranges(&location, &ranges).await.unwrap()
            == vec![Bytes::from_static(b"ab"), Bytes::from_static(b"ef")]
    );
    assert2::assert!(store.reads.load(Ordering::SeqCst) >= 5);
}

#[tokio::test]
async fn forwards_multipart_listing_copy_options_and_bulk_delete() {
    let store = store();
    let location = Path::from("prefix/object");
    let copy = Path::from("prefix/copy");
    let mut upload = store.put_multipart(&location).await.unwrap();
    upload
        .put_part(Bytes::from_static(b"multipart").into())
        .await
        .unwrap();
    upload.complete().await.unwrap();
    store
        .copy_opts(&location, &copy, CopyOptions::default())
        .await
        .unwrap();
    assert2::assert!(
        store.get(&copy).await.unwrap().bytes().await.unwrap() == Bytes::from_static(b"multipart")
    );
    assert2::assert!(matches!(
        store
            .copy_opts(
                &location,
                &copy,
                CopyOptions {
                    mode: object_store::CopyMode::Create,
                    ..Default::default()
                }
            )
            .await,
        Err(object_store::Error::AlreadyExists { .. })
    ));
    let prefix = Path::from("prefix");
    let list = store
        .list(Some(&prefix))
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert2::assert!(list.len() == 2);
    assert2::assert!(
        store
            .list_with_delimiter(Some(&prefix))
            .await
            .unwrap()
            .objects
            .len()
            == 2
    );
    let list_after = store
        .list_with_offset(Some(&prefix), &copy)
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert2::assert!(list_after.len() == 1);
    assert2::assert!(list_after[0].location == location);
    let locations = futures::stream::iter([Ok(location.clone()), Ok(copy.clone())]).boxed();
    let removed = store
        .delete_stream(locations)
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert2::assert!(removed == vec![location, copy]);
    assert2::assert!(
        store
            .list(None)
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn preserves_a_custom_listing_after_an_async_override() {
    let store = store();
    let visible = Path::from("prefix/visible");
    let hidden = Path::from("prefix/hidden");
    for path in [&visible, &hidden] {
        store
            .put(path, Bytes::from_static(b"value").into())
            .await
            .unwrap();
    }
    let prefix = Path::from("prefix");
    let listed = store
        .list(Some(&prefix))
        .map_ok(|meta| meta.location)
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert2::assert!(listed == vec![visible]);
    assert2::assert!(store.listings.load(Ordering::SeqCst) == 1);
    assert2::assert!(store.inner.head(&hidden).await.is_ok());
}
