use super::Deserialize;

/// Deserialize a `u64` that the querier may encode as a JSON number **or** a
/// string. Tempo encodes some accounting counters as strings.
pub(crate) fn de_u64_lenient<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::Deserialize as _;

    #[derive(Deserialize)]
    #[serde(untagged)]
    enum NumOrStr {
        Num(u64),
        Str(String),
    }

    match NumOrStr::deserialize(deserializer)? {
        NumOrStr::Num(n) => Ok(n),
        NumOrStr::Str(s) => Ok(s.parse().unwrap_or(0)),
    }
}
