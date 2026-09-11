use super::{
    IndexShardObject, IndexShardRange, parse_shard_bound_key, unescape_object_path_segment,
};

/// Classifies an object listed under the index-shard prefix of a key.
///
/// Returns the tenant it belongs to and what it is. The tenant segment comes
/// back through [`unescape_object_path_segment`], so the caller gets the name
/// the writer was given and not the escaped form. Anything that does not
/// spell one of the two shapes this module writes comes back as
/// [`IndexShardObject::Foreign`], so a caller neither decodes it nor deletes
/// it: the prefix belongs to the index, but a bucket is shared and being wrong
/// about that costs somebody else their object.
pub(crate) fn parse_index_shard_location(
    shards_prefix: &str,
    location: &str,
) -> Option<(String, IndexShardObject)> {
    let rest = location
        .strip_prefix(shards_prefix)?
        .trim_start_matches('/');
    let (tenant_segment, rest) = rest.split_once('/')?;
    let tenant = unescape_object_path_segment(tenant_segment.strip_prefix("tenant=")?)?;

    if rest == "unbound.kbi" {
        return Some((tenant, IndexShardObject::UnboundSeries));
    }

    let mut parts = rest.split('/');
    let (Some(span), Some("shard.kbi"), None) = (parts.next(), parts.next(), parts.next()) else {
        return Some((tenant, IndexShardObject::Foreign));
    };
    let Some(span) = span.strip_prefix("time=") else {
        return Some((tenant, IndexShardObject::Foreign));
    };
    // Both bounds are fixed-width, so the separator sits at a known offset and
    // there is no need to guess which `-` divides them.
    let Some((start, end)) = span.split_once('-') else {
        return Some((tenant, IndexShardObject::Foreign));
    };
    let object = match (parse_shard_bound_key(start), parse_shard_bound_key(end)) {
        (Some(start), Some(end)) if start <= end => {
            IndexShardObject::Shard(IndexShardRange::new(start, end))
        }
        _ => IndexShardObject::Foreign,
    };
    Some((tenant, object))
}
