use super::ToPrimitive;

pub(crate) fn seconds_to_ms(value: &str) -> Result<i64, ()> {
    let seconds = value.parse::<f64>().map_err(|_| ())?;
    (seconds * 1000.0).round().to_i64().ok_or(())
}
