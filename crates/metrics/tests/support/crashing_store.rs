//! An in-memory object store that stops like a crashed process.

use std::sync::Mutex;

use futures::{StreamExt as _, stream::BoxStream};
use object_store::{
    CopyOptions, MultipartUpload, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    memory::InMemory, path::Path,
};

/// Where a [`CrashingStore`] stops: at the `occurrence`th write, counted
/// from one, of a key that contains `key_part`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CrashPoint {
    pub key_part: String,
    pub occurrence: usize,
}

#[derive(Debug, Default)]
struct CrashState {
    point: Option<CrashPoint>,
    seen: usize,
    crashed: bool,
}

/// An in-memory store that refuses the write at its crash point, and every
/// write and delete after it, until the test clears the crash.
///
/// The refused writes and deletes model a process that stopped: it cleans up
/// nothing. Reads go on, as the reads of other processes do.
#[derive(Debug, Default)]
pub struct CrashingStore {
    inner: InMemory,
    state: Mutex<CrashState>,
}

impl CrashingStore {
    /// Sets the crash point, and clears an earlier crash.
    pub fn crash_at(&self, point: Option<CrashPoint>) {
        *self.state.lock().expect("the crash state") = CrashState {
            point,
            ..CrashState::default()
        };
    }

    /// Whether the store reached its crash point.
    pub fn crashed(&self) -> bool {
        self.state.lock().expect("the crash state").crashed
    }

    fn write(&self, location: &Path) -> object_store::Result<()> {
        let mut state = self.state.lock().expect("the crash state");
        let occurrence = state
            .point
            .as_ref()
            .filter(|point| location.as_ref().contains(&point.key_part))
            .map(|point| point.occurrence);
        if !state.crashed
            && let Some(occurrence) = occurrence
        {
            state.seen += 1;
            state.crashed = state.seen == occurrence;
        }
        if state.crashed {
            Err(crashed(location))
        } else {
            Ok(())
        }
    }
}

fn crashed(location: &Path) -> object_store::Error {
    object_store::Error::Generic {
        store: "CrashingStore",
        source: format!("the process stopped before it wrote {location}").into(),
    }
}

impl std::fmt::Display for CrashingStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("CrashingStore")
    }
}

krabka_blockstore::delegate_object_store! {
    CrashingStore => inner;
    forward [get_opts, list, list_with_delimiter];

    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.write(location)?;
        self.inner.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.write(location)?;
        self.inner.put_multipart_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        if self.crashed() {
            locations
                .map(|location| location.and_then(|location| Err(crashed(&location))))
                .boxed()
        } else {
            self.inner.delete_stream(locations)
        }
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.write(to)?;
        self.inner.copy_opts(from, to, options).await
    }
}
