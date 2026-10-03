use super::{
    Arc, DEFAULT_BLOCK_READ_MAX, ObjectStore, SerdeCompat, StorageAuditError, SymbolTableShape,
    WincodeDeserialize, read_capped_object,
};

/// Decodes the `.symdb` symbol table at `key` the way `SymbolDb::decode`
/// does, and returns why it does not decode.
///
/// Returns `None` for a symbol table that decodes or that is gone.
///
/// # Errors
/// Returns [`StorageAuditError::ObjectStore`] when the read fails for a
/// reason other than absence, or the object is larger than
/// [`DEFAULT_BLOCK_READ_MAX`].
pub async fn check_symbol_table(
    store: &Arc<dyn ObjectStore>,
    key: &str,
) -> Result<Option<String>, StorageAuditError> {
    let Some(bytes) = read_capped_object(store, key, DEFAULT_BLOCK_READ_MAX).await? else {
        return Ok(None);
    };
    Ok(
        <SerdeCompat<SymbolTableShape> as WincodeDeserialize>::deserialize(&bytes)
            .err()
            .map(|error| error.to_string()),
    )
}
