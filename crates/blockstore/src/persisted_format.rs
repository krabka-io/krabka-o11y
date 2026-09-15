use parquet::file::{metadata::ParquetMetaData, properties::WriterProperties};

pub const PERSISTED_BLOCK_FORMAT_KEY: &str = "krabka.format.version";
pub const PERSISTED_BLOCK_FORMAT_VERSION: &str = "1";

#[must_use]
pub(crate) fn persisted_block_writer_properties() -> WriterProperties {
    WriterProperties::builder()
        .set_key_value_metadata(Some(vec![parquet::file::metadata::KeyValue::new(
            PERSISTED_BLOCK_FORMAT_KEY.to_string(),
            Some(PERSISTED_BLOCK_FORMAT_VERSION.to_string()),
        )]))
        .build()
}

/// Missing metadata is the supported legacy version. A present version must
/// be unique and exactly match the current reader.
///
/// # Errors
/// Returns a description when the version marker is malformed or unsupported.
pub fn validate_persisted_block_format(metadata: &ParquetMetaData) -> Result<(), String> {
    let versions = metadata
        .file_metadata()
        .key_value_metadata()
        .into_iter()
        .flatten()
        .filter(|entry| entry.key == PERSISTED_BLOCK_FORMAT_KEY)
        .map(|entry| entry.value.as_deref())
        .collect::<Vec<_>>();
    match versions.as_slice() {
        [] | [Some(PERSISTED_BLOCK_FORMAT_VERSION)] => Ok(()),
        [None] => Err("persisted block format version has no value".to_string()),
        [Some(version)] => Err(format!(
            "unsupported persisted block format version `{version}`"
        )),
        _ => Err("persisted block format version appears more than once".to_string()),
    }
}
