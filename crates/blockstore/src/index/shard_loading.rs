use super::*;

/// What an object under the index-shard prefix turned out to be.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum IndexShardObject {
    /// A shard covering the given span.
    Shard(IndexShardRange),
    /// The tenant's series that no block carries yet.
    UnboundSeries,
    /// Not written by this module and neither read nor deleted.
    Foreign,
}

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

/// Reads the shards of `key` that a query needs and folds them into one index.
///
/// `tenant` narrows the listing to one tenant's prefix, and `window` drops the
/// shards whose span cannot meet the query's. What is left is what gets read,
/// which is the whole point of the layout: the shards outside the window are
/// never fetched and never held.
///
/// [`super::MAX_INDEX_SHARDS_PER_TENANT`] bounds a tenant's shards, so the
/// listing is one page and an offset would save nothing. An offset would also
/// have to guess how wide the first shard is. That guess is what makes such a
/// skip unsound.
#[instrument(
    level = "debug",
    skip_all,
    fields(
        key = %key,
        tenant = tracing::field::Empty,
        listed = tracing::field::Empty,
        read = tracing::field::Empty,
    ),
    err
)]
pub(crate) async fn load_index_shards(
    store: &Arc<dyn ObjectStore>,
    key: &str,
    tenant: Option<&str>,
    window: Option<super::IndexShardRange>,
    max_bytes: ByteSize,
) -> Result<Index> {
    if let Some(tenant) = tenant {
        tracing::Span::current().record("tenant", tenant);
    }
    let shards_prefix = index_shards_prefix_for_key(key);
    let listing_prefix = match tenant {
        Some(tenant) => index_shard_tenant_prefix(key, tenant),
        None => shards_prefix.clone(),
    };

    let mut listed = 0_usize;
    let mut wanted = Vec::new();
    let mut listing = store.list(Some(&Path::from(listing_prefix)));
    while let Some(meta) = listing.next().await {
        let location = meta?.location;
        let Some(parsed) = parse_index_shard_location(&shards_prefix, location.as_ref()) else {
            continue;
        };
        match parsed.object {
            IndexShardObject::Shard(range) => {
                listed += 1;
                if window.is_none_or(|query| range.overlaps(query.start, query.end)) {
                    wanted.push(location);
                }
            }
            // A series no block carries yet has no span to compare against, so
            // it is read whatever the window is.
            IndexShardObject::UnboundSeries => {
                listed += 1;
                wanted.push(location);
            }
            IndexShardObject::Foreign => {}
        }
    }
    tracing::Span::current().record("listed", listed);
    tracing::Span::current().record("read", wanted.len());

    let mut index = Index::new();
    for location in wanted {
        let bytes = read_index_shard(store, &location, max_bytes).await?;
        let shard = decode_index_shard(location.as_ref(), &bytes)?;
        index.merge_from(&shard);
    }
    Ok(index)
}
