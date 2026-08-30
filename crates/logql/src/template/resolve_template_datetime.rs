use super::{NaiveDateTime, Utc, FixedOffset, TimeZone, Tz, LocalResult};

pub(crate) fn resolve_template_datetime(
    datetime: NaiveDateTime,
    zone: &str,
    offset_seconds: Option<i32>,
) -> Option<chrono::DateTime<Utc>> {
    if let Some(offset_seconds) = offset_seconds {
        let offset = FixedOffset::east_opt(offset_seconds)?;
        return offset
            .from_local_datetime(&datetime)
            .single()
            .map(|datetime| datetime.with_timezone(&Utc));
    }
    if zone == "UTC" || zone == "Local" {
        return Some(Utc.from_utc_datetime(&datetime));
    }
    let zone = zone.parse::<Tz>().ok()?;
    match zone.from_local_datetime(&datetime) {
        LocalResult::Single(datetime) => Some(datetime.with_timezone(&Utc)),
        LocalResult::Ambiguous(earliest, _) => Some(earliest.with_timezone(&Utc)),
        LocalResult::None => None,
    }
}
