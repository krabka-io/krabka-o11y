pub(crate) fn unix_ms_to_rfc3339(timestamp_ms: i64) -> String {
    use time::format_description::well_known::Rfc3339;

    let Ok(time) =
        time::OffsetDateTime::from_unix_timestamp_nanos(i128::from(timestamp_ms) * 1_000_000)
    else {
        return "1970-01-01T00:00:00Z".to_string();
    };
    time.format(&Rfc3339)
        .unwrap_or_else(|_| "1970-01-01T00:00:00Z".to_string())
}
