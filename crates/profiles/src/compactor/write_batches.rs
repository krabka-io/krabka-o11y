use super::*;

pub(crate) async fn write_batches(
    store: &Arc<dyn ObjectStore>,
    output_key: &str,
    batches: &[RecordBatch],
) -> Result<(), ProfilesError> {
    let Some(first) = batches.first() else {
        return Err(ProfilesError::Block(
            "cannot compact empty block set".to_string(),
        ));
    };
    let mut bytes = Cursor::new(Vec::new());
    {
        let mut writer = ArrowWriter::try_new(&mut bytes, first.schema(), None)
            .map_err(|err| ProfilesError::Block(err.to_string()))?;
        for batch in batches {
            writer
                .write(batch)
                .map_err(|err| ProfilesError::Block(err.to_string()))?;
        }
        writer
            .close()
            .map_err(|err| ProfilesError::Block(err.to_string()))?;
    }
    store
        .put(
            &Path::from(output_key),
            PutPayload::from(bytes.into_inner()),
        )
        .await
        .map_err(|err| ProfilesError::Block(err.to_string()))?;
    Ok(())
}
