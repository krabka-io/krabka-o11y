use super::{OffsetDateTime, Rfc3339};

pub(crate) fn rfc3339_to_ms(value: &str) -> Result<i64, ()> {
    let time = OffsetDateTime::parse(value, &Rfc3339).map_err(|_| ())?;
    i64::try_from(time.unix_timestamp_nanos() / 1_000_000).map_err(|_| ())
}
