use std::collections::HashMap;

use object_store::ObjectStoreExt as _;

use super::{Arc, NativeResolver, NativeSymbol, ObjectStore, Path, ProfileIndex, SymbolizeRequest};

struct UploadedResolver<'a> {
    uploaded: HashMap<String, krabka_pprof::ObjectSymbolResolver>,
    configured: &'a dyn NativeResolver,
}

impl NativeResolver for UploadedResolver<'_> {
    fn symbolize(&self, request: &SymbolizeRequest) -> Option<Vec<NativeSymbol>> {
        self.uploaded
            .get(&request.build_id)
            .and_then(|resolver| resolver.symbolize(request))
            .or_else(|| self.configured.symbolize(request))
    }
}

/// Resolve unsymbolized locations in every indexed block and persist the
/// updated symbol database beside that block.
///
/// # Errors
/// Returns an error when a block or its symbol database cannot be read, decoded, or written.
pub async fn symbolize_blocks_once(
    store: &Arc<dyn ObjectStore>,
    index: &ProfileIndex,
    resolver: &dyn NativeResolver,
) -> Result<usize, crate::ProfilesError> {
    let mut updated = 0;
    for block in index.all_blocks() {
        let key = Path::from(format!("{}.symdb", block.object_key));
        let bytes = store
            .get(&key)
            .await
            .map_err(|error| crate::ProfilesError::Block(error.to_string()))?
            .bytes()
            .await
            .map_err(|error| crate::ProfilesError::Block(error.to_string()))?;
        let mut symbols = krabka_pprof::SymbolDb::decode(&bytes)?;
        let mut uploaded = HashMap::new();
        for request in symbols.pending_native_symbols() {
            if request.build_id.is_empty()
                || uploaded.contains_key(&request.build_id)
                || !request.build_id.chars().all(|ch| ch.is_ascii_hexdigit())
            {
                continue;
            }
            let object = match store
                .get(&Path::from(format!("debuginfo/{}", request.build_id)))
                .await
            {
                Ok(object) if object.meta.size <= 512 * 1024 * 1024 => object,
                _ => continue,
            };
            let Ok(bytes) = object.bytes().await else {
                continue;
            };
            if let Ok(object) = krabka_pprof::ObjectSymbolResolver::from_bytes(&bytes) {
                uploaded.insert(request.build_id, object);
            }
        }
        let resolver = UploadedResolver {
            uploaded,
            configured: resolver,
        };
        let block_updated = symbols.symbolize_native(&resolver);
        if block_updated == 0 {
            continue;
        }
        store
            .put(&key, symbols.encode().into())
            .await
            .map_err(|error| crate::ProfilesError::Block(error.to_string()))?;
        updated += block_updated;
    }
    Ok(updated)
}
