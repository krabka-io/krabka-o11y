use super::{parse_shard_bound_key, shard_payload_prefix_for_key, unescape_object_path_segment};

/// Whether `location` is a shard payload of the index stored under `key`.
///
/// Anything that does not spell the shape [`super::shard_payload_object_key`]
/// writes is foreign, and the sweep neither reads nor deletes it. A tenant
/// segment whose escape does not read back is foreign too: the prefix
/// belongs to the index, but a bucket is shared and being wrong about that
/// costs somebody else their object.
pub(crate) fn is_shard_payload_location(key: &str, location: &str) -> bool {
    let prefix = shard_payload_prefix_for_key(key);
    let Some(rest) = location.strip_prefix(&prefix) else {
        return false;
    };
    let mut parts = rest.trim_start_matches('/').split('/');
    let (Some(tenant), Some(span), Some(file), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return false;
    };
    let is_tenant_segment = tenant
        .strip_prefix("tenant=")
        .is_some_and(|tenant| unescape_object_path_segment(tenant).is_some());
    if !is_tenant_segment
        || !std::path::Path::new(file)
            .extension()
            .is_some_and(|extension| extension.eq_ignore_ascii_case("kbs"))
    {
        return false;
    }
    let Some(span) = span.strip_prefix("time=") else {
        return false;
    };
    // Both bounds are fixed-width, so the separator sits at a known offset and
    // there is no need to guess which `-` divides them.
    let Some((start, end)) = span.split_once('-') else {
        return false;
    };
    matches!(
        (parse_shard_bound_key(start), parse_shard_bound_key(end)),
        (Some(start), Some(end)) if start <= end
    )
}
