use super::{
    IndexShardObject, IndexShardRange, parse_shard_bound_key, unescape_object_path_segment,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ParsedIndexShardLocation {
    pub tenant: String,
    pub object: IndexShardObject,
}

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
) -> Option<ParsedIndexShardLocation> {
    let rest = location
        .strip_prefix(shards_prefix)?
        .trim_start_matches('/');
    let segments = rest.split_once('/')?;
    let tenant_segment = segments.0;
    let rest = segments.1;
    let tenant = unescape_object_path_segment(tenant_segment.strip_prefix("tenant=")?)?;

    if rest == "unbound.kbi" {
        return Some(ParsedIndexShardLocation {
            tenant,
            object: IndexShardObject::UnboundSeries,
        });
    }

    let mut parts = rest.split('/');
    let span = parts.next();
    let kind = parts.next();
    let extra = parts.next();
    if span.is_none() || kind != Some("shard.kbi") || extra.is_some() {
        return Some(ParsedIndexShardLocation {
            tenant,
            object: IndexShardObject::Foreign,
        });
    }
    let Some(span) = span else { unreachable!() };
    let Some(span) = span.strip_prefix("time=") else {
        return Some(ParsedIndexShardLocation {
            tenant,
            object: IndexShardObject::Foreign,
        });
    };
    // Both bounds are fixed-width, so the separator sits at a known offset and
    // there is no need to guess which `-` divides them.
    let Some(bounds) = span.split_once('-') else {
        return Some(ParsedIndexShardLocation {
            tenant,
            object: IndexShardObject::Foreign,
        });
    };
    let object = if let Some(start) = parse_shard_bound_key(bounds.0) {
        if let Some(end) = parse_shard_bound_key(bounds.1) {
            if start <= end {
                IndexShardObject::Shard(IndexShardRange::new(start, end))
            } else {
                IndexShardObject::Foreign
            }
        } else {
            IndexShardObject::Foreign
        }
    } else {
        IndexShardObject::Foreign
    };
    Some(ParsedIndexShardLocation { tenant, object })
}
