use super::*;

pub(crate) async fn collect_tenant_log_index_shard_ranges(
    shard_prefix: ObjectPath,
    mut stream: futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>,
    filter_range: Option<TimeRange>,
) -> Result<Vec<TimeRange>, BlockStoreError> {
    let mut ranges = Vec::new();
    while let Some(meta) = stream.next().await {
        let meta = meta?;
        if let Some(range) =
            parse_log_tenant_index_shard_range_from_object_path(&shard_prefix, &meta.location)
            && filter_range.is_none_or(|filter_range| range.overlaps(filter_range))
        {
            ranges.push(range);
        }
    }

    ranges.sort_by_key(|range| (range.start_ns, range.end_ns));
    ranges.dedup();
    Ok(ranges)
}
