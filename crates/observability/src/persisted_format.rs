/// Kafka header carried by every Krabka-owned durable record.
pub const PERSISTED_FORMAT_HEADER: &str = "krabka-format-version";
pub const PERSISTED_FORMAT_VERSION: &[u8] = b"1";

#[derive(Debug, thiserror::Error, Eq, PartialEq)]
pub enum PersistedFormatError {
    #[error("persisted format header appears more than once")]
    Duplicate,
    #[error("persisted format header has no value")]
    MissingValue,
    #[error("unsupported persisted format version `{0}`")]
    Unsupported(String),
}

/// Accepts legacy records without a header as version 1 and rejects unknown
/// versions before callers decode or mutate state.
///
/// # Errors
/// Returns an error when the version header is malformed or unsupported.
pub fn validate_persisted_format<'a>(
    headers: impl IntoIterator<Item = (&'a str, Option<&'a [u8]>)>,
) -> Result<(), PersistedFormatError> {
    let mut version = None;
    for (key, value) in headers {
        if key != PERSISTED_FORMAT_HEADER {
            continue;
        }
        if version.replace(value).is_some() {
            return Err(PersistedFormatError::Duplicate);
        }
    }
    match version {
        None | Some(Some(PERSISTED_FORMAT_VERSION)) => Ok(()),
        Some(None) => Err(PersistedFormatError::MissingValue),
        Some(Some(value)) => Err(PersistedFormatError::Unsupported(
            String::from_utf8_lossy(value).into_owned(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    #[test]
    fn legacy_and_current_are_supported_but_future_is_not() {
        check!(validate_persisted_format([]).is_ok());
        check!(
            validate_persisted_format([(PERSISTED_FORMAT_HEADER, Some(b"1".as_slice()))]).is_ok()
        );
        check!(
            validate_persisted_format([(PERSISTED_FORMAT_HEADER, Some(b"2".as_slice()))])
                == Err(PersistedFormatError::Unsupported("2".to_string()))
        );
    }
}
