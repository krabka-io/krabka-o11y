use super::*;

pub(crate) fn kafka_headers_with_prefix(
    headers: &[KafkaWalHeader],
    prefix: &str,
    duplicate_error: impl Fn(String) -> WalRecordDecodeError,
) -> Result<BTreeMap<String, String>, WalRecordDecodeError> {
    let mut values = BTreeMap::new();
    for header in headers {
        let Some(name) = header.key.strip_prefix(prefix) else {
            continue;
        };
        let value = header.value.as_ref().ok_or_else(|| {
            WalRecordDecodeError::MissingNativeHeaderValue {
                name: header.key.clone(),
            }
        })?;
        let value = String::from_utf8(value.clone()).map_err(|_| {
            WalRecordDecodeError::InvalidNativeHeaderUtf8 {
                name: header.key.clone(),
            }
        })?;
        let name = name.to_string();
        if values.insert(name.clone(), value).is_some() {
            return Err(duplicate_error(name));
        }
    }
    Ok(values)
}
