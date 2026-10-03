/// Kafka header carried by every Krabka-owned durable record.
pub const PERSISTED_FORMAT_HEADER: &str = "krabka-format-version";
pub const PERSISTED_FORMAT_VERSION: &[u8] = b"1";

#[derive(Debug, thiserror::Error, Eq, PartialEq)]
#[non_exhaustive]
pub enum PersistedFormatError {
    #[error("persisted format header is missing")]
    Missing,
    #[error("persisted format header appears more than once")]
    Duplicate,
    #[error("persisted format header has no value")]
    MissingValue,
    #[error("unsupported persisted format version `{0}`")]
    Unsupported(String),
}

/// Checks the format version header of a Krabka-owned durable record.
///
/// The header is required. It must appear once and exactly match the version
/// that this build writes. Callers check it before they decode a record or
/// change state.
///
/// # Errors
/// Returns an error when the version header is absent, malformed, or
/// unsupported.
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
        Some(Some(PERSISTED_FORMAT_VERSION)) => Ok(()),
        None => Err(PersistedFormatError::Missing),
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
    fn only_the_current_version_is_supported() {
        struct Case {
            name: &'static str,
            values: Vec<Option<&'static [u8]>>,
            expected: Result<(), PersistedFormatError>,
        }

        let cases = [
            Case {
                name: "current",
                values: vec![Some(b"1")],
                expected: Ok(()),
            },
            Case {
                name: "absent",
                values: vec![],
                expected: Err(PersistedFormatError::Missing),
            },
            Case {
                name: "future",
                values: vec![Some(b"2")],
                expected: Err(PersistedFormatError::Unsupported("2".to_string())),
            },
            Case {
                name: "no value",
                values: vec![None],
                expected: Err(PersistedFormatError::MissingValue),
            },
            Case {
                name: "duplicate",
                values: vec![Some(b"1"), Some(b"1")],
                expected: Err(PersistedFormatError::Duplicate),
            },
        ];
        for case in cases {
            let headers = case
                .values
                .iter()
                .map(|value| (PERSISTED_FORMAT_HEADER, *value));
            check!(
                validate_persisted_format(headers) == case.expected,
                "{}",
                case.name
            );
        }
    }
}
