use super::*;

pub(crate) async fn load_symdb(
    store: &Arc<dyn ObjectStore>,
    block_key: &str,
) -> Result<SymbolDb, ProfilesError> {
    let bytes = store
        .get(&Path::from(format!("{block_key}.symdb")))
        .await
        .map_err(|err| ProfilesError::Block(err.to_string()))?
        .bytes()
        .await
        .map_err(|err| ProfilesError::Block(err.to_string()))?;
    SymbolDb::decode(&bytes).map_err(|err| ProfilesError::Block(err.to_string()))
}
