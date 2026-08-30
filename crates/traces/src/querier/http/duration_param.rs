use super::*;

pub(crate) fn duration_param(uri: &Uri, key: &str) -> Result<Option<Time>, String> {
    query_param(uri, key)
        .map(|value| {
            parse_go_duration_ns(&value)
                .map(|nanos| Time::from_nanos(i64::try_from(nanos).unwrap_or(i64::MAX)))
                .map_err(|err| format!("invalid {key}: {err}"))
        })
        .transpose()
}
