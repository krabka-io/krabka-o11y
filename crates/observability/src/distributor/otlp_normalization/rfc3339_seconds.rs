use super::*;

pub(crate) fn rfc3339_seconds(timestamp_ns: i64) -> String {
    let seconds = timestamp_ns.div_euclid(1_000_000_000);
    let Ok(timestamp) = OffsetDateTime::from_unix_timestamp(seconds) else {
        return seconds.to_string();
    };
    let date = timestamp.date();
    let time = timestamp.time();
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        date.year(),
        u8::from(date.month()),
        date.day(),
        time.hour(),
        time.minute(),
        time.second()
    )
}
