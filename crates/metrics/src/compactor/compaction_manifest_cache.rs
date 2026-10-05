use bytes::Bytes;

use super::{
    Arc, BTreeMap, BTreeSet, CompactionIndexManifest, CompactionManifestError, ObjectMeta,
    ObjectStore, Path, list_compaction_index_objects, read_compaction_manifests,
};

/// Bounded manifest contents for one object store, checked against each fresh listing.
///
/// Only objects with an ETag or version can hit. Decoding and listed-key
/// validation still run on every read. The cache holds at most one MiB of
/// content and 128 entries; larger objects keep the ordinary read path.
pub struct CompactionManifestCache {
    pub(super) store: Arc<dyn ObjectStore>,
    entries: BTreeMap<Path, (ObjectMeta, Bytes)>,
    bytes: usize,
}

impl CompactionManifestCache {
    #[must_use]
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self {
            store,
            entries: BTreeMap::new(),
            bytes: 0,
        }
    }

    /// Lists current live manifests, then reads and validates their contents.
    ///
    /// # Errors
    /// Propagates listing errors and read, decode or key errors in loaded contents.
    pub async fn list(&mut self) -> Result<Vec<CompactionIndexManifest>, CompactionManifestError> {
        let store = Arc::clone(&self.store);
        let listing = list_compaction_index_objects(&store).await?;
        read_compaction_manifests(&store, listing.live, Some(self)).await
    }

    pub(super) fn get(&self, object: &ObjectMeta) -> Option<Bytes> {
        self.entries
            .get(&object.location)
            .filter(|(meta, _)| has_identity(object) && meta == object)
            .map(|(_, bytes)| bytes.clone())
    }

    pub(super) fn retain(&mut self, live: &BTreeSet<&str>) {
        self.entries.retain(|key, _| live.contains(key.as_ref()));
        self.bytes = self.entries.values().map(|(_, bytes)| bytes.len()).sum();
    }

    pub(super) fn insert(&mut self, meta: ObjectMeta, fetched: &ObjectMeta, bytes: Bytes) {
        if let Some((_, old)) = self.entries.remove(&meta.location) {
            self.bytes -= old.len();
        }
        if has_identity(&meta)
            && content_matches(&meta, fetched)
            && self.entries.len() < 128
            && self.bytes.saturating_add(bytes.len()) <= 1024 * 1024
        {
            self.bytes += bytes.len();
            self.entries.insert(meta.location.clone(), (meta, bytes));
        }
    }
}

// HTTP GET dates can lose the fractional precision of a listing. The fetched
// content must still match every supplied strong validator and the same second.
// Store the listing identity; later hits compare its complete metadata exactly.
fn content_matches(listed: &ObjectMeta, fetched: &ObjectMeta) -> bool {
    listed.location == fetched.location
        && listed.size == fetched.size
        && listed.last_modified.timestamp() == fetched.last_modified.timestamp()
        && listed
            .e_tag
            .as_ref()
            .filter(|value| !value.is_empty())
            .is_none_or(|value| fetched.e_tag.as_ref() == Some(value))
        && listed
            .version
            .as_ref()
            .filter(|value| !value.is_empty())
            .is_none_or(|value| fetched.version.as_ref() == Some(value))
}

fn has_identity(meta: &ObjectMeta) -> bool {
    meta.e_tag.as_ref().is_some_and(|value| !value.is_empty())
        || meta.version.as_ref().is_some_and(|value| !value.is_empty())
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use object_store::{ObjectStoreExt as _, memory::InMemory};

    use super::*;

    #[tokio::test]
    async fn content_needs_complete_identity_and_a_bounded_admission() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("metrics/example.index");
        store
            .put(&path, Bytes::from_static(b"abc").into())
            .await
            .unwrap();
        let original = store.head(&path).await.unwrap();
        let mut cache = CompactionManifestCache::new(store);
        cache.insert(original.clone(), &original, Bytes::from_static(b"abc"));
        assert!(cache.get(&original) == Some(Bytes::from_static(b"abc")));
        let mut listed = original.clone();
        listed.last_modified -= std::time::Duration::from_nanos(u64::from(
            listed.last_modified.timestamp_subsec_nanos(),
        ));
        cache.insert(listed.clone(), &original, Bytes::from_static(b"abc"));
        assert!(cache.get(&listed) == Some(Bytes::from_static(b"abc")));
        let mut replaced = original.clone();
        replaced.e_tag = Some("changed-after-listing".to_owned());
        cache.insert(listed.clone(), &replaced, Bytes::from_static(b"new"));
        assert!(cache.get(&listed).is_none());
        cache.insert(original.clone(), &original, Bytes::from_static(b"abc"));
        let mut changed = original.clone();
        changed.size += 1;
        assert!(cache.get(&changed).is_none());
        changed = original.clone();
        changed.last_modified += std::time::Duration::from_secs(1);
        assert!(cache.get(&changed).is_none());
        changed = original.clone();
        changed.version = Some("replacement".to_owned());
        assert!(cache.get(&changed).is_none());
        changed = original.clone();
        changed.e_tag = None;
        changed.version = None;
        cache.insert(changed.clone(), &changed, Bytes::from_static(b"abc"));
        assert!(cache.get(&changed).is_none());
        changed.e_tag = Some(String::new());
        changed.version = Some(String::new());
        cache.insert(changed.clone(), &changed, Bytes::from_static(b"abc"));
        assert!(cache.get(&changed).is_none());

        cache.insert(
            original.clone(),
            &original,
            Bytes::from(vec![0; 1024 * 1024 + 1]),
        );
        assert!(cache.get(&original).is_none());
        let mut last = original.clone();
        for item in 0..129 {
            last.location = Path::from(format!("metrics/{item:03}.index"));
            cache.insert(last.clone(), &last, Bytes::from_static(b"abc"));
        }
        assert!(cache.get(&last).is_none());
        cache.retain(&BTreeSet::new());
        cache.insert(original.clone(), &original, Bytes::from_static(b"abc"));
        assert!(cache.get(&original) == Some(Bytes::from_static(b"abc")));
    }
}
