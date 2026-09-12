use object_store::ObjectMeta;

use super::{Arc, ObjectStore, ObjectStoreExt, Path, ProfilesError, SymbolDb, symdb_key};

pub(crate) async fn load_symdb(
    store: &Arc<dyn ObjectStore>,
    block_key: &str,
) -> Result<(ObjectMeta, SymbolDb), ProfilesError> {
    let result = store
        .get(&Path::from(symdb_key(block_key)))
        .await
        .map_err(|err| ProfilesError::Block(err.to_string()))?;
    let meta = result.meta.clone();
    let bytes = result
        .bytes()
        .await
        .map_err(|err| ProfilesError::Block(err.to_string()))?;
    let symbols = SymbolDb::decode(&bytes).map_err(|err| ProfilesError::Block(err.to_string()))?;
    Ok((meta, symbols))
}
