use super::{DefaultHasher, Hash as _, Hasher as _, ObjectMeta};

/// Adds the exact input object identities to a planned compaction key.
#[must_use]
pub fn versioned_compaction_key(key: &str, inputs: &[ObjectMeta]) -> String {
    let mut hasher = DefaultHasher::new();
    for input in inputs {
        input.location.hash(&mut hasher);
        input.size.hash(&mut hasher);
        input.e_tag.hash(&mut hasher);
        input.version.hash(&mut hasher);
        input.last_modified.hash(&mut hasher);
    }
    let suffix = format!("-{:016x}", hasher.finish());
    key.strip_suffix(".parquet").map_or_else(
        || format!("{key}{suffix}"),
        |stem| format!("{stem}{suffix}.parquet"),
    )
}

#[cfg(test)]
mod tests {
    use object_store::{ObjectStoreExt, PutPayload, memory::InMemory, path::Path};

    use super::*;

    #[tokio::test]
    async fn reused_input_versions_produce_different_output_keys() {
        let store = InMemory::new();
        let path = Path::from("input.parquet");
        store
            .put(&path, PutPayload::from_static(b"old"))
            .await
            .unwrap();
        let old = store.head(&path).await.unwrap();
        store
            .put(&path, PutPayload::from_static(b"new"))
            .await
            .unwrap();
        let new = store.head(&path).await.unwrap();

        assert2::assert!(
            versioned_compaction_key("output.parquet", &[old])
                != versioned_compaction_key("output.parquet", &[new])
        );
    }
}
