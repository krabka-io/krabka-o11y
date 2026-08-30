use super::*;

pub(crate) fn required_kafka_header_utf8(
    headers: &[KafkaWalHeader],
    name: &str,
) -> Result<String, WalRecordDecodeError> {
    optional_kafka_header_utf8(headers, name)?.ok_or_else(|| {
        WalRecordDecodeError::MissingNativeHeader {
            name: name.to_string(),
        }
    })
}
