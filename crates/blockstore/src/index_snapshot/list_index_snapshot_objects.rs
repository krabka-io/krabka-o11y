use super::*;

#[instrument(level = "debug", skip_all, fields(key = %key), err)]
pub async fn list_index_snapshot_objects(
    store: &Arc<dyn ObjectStore>,
    key: &str,
) -> Result<Vec<ObjectMeta>> {
    let prefix = Path::from(index_snapshot_prefix_for_key(key));
    let mut stream = store.list(Some(&prefix));
    let mut objects = Vec::new();
    while let Some(meta) = stream.next().await {
        let meta = meta?;
        if std::path::Path::new(meta.location.as_ref())
            .extension()
            .is_some_and(|ext| ext.eq_ignore_ascii_case("json"))
        {
            objects.push(meta);
        }
    }
    objects.sort_by(|a, b| a.location.as_ref().cmp(b.location.as_ref()));
    Ok(objects)
}
