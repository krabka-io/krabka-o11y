use super::{KafkaWalHeader, WalRecordDecodeError};

pub(crate) fn optional_kafka_header_utf8(
    headers: &[KafkaWalHeader],
    name: &str,
) -> Result<Option<String>, WalRecordDecodeError> {
    let Some(header) = headers.iter().find(|header| header.key == name) else {
        return Ok(None);
    };
    let value =
        header
            .value
            .as_ref()
            .ok_or_else(|| WalRecordDecodeError::MissingNativeHeaderValue {
                name: name.to_string(),
            })?;
    String::from_utf8(value.clone()).map(Some).map_err(|_| {
        WalRecordDecodeError::InvalidNativeHeaderUtf8 {
            name: name.to_string(),
        }
    })
}
