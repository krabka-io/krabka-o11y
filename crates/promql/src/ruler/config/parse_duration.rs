use super::{Time, PromqlError, TimeExt};

/// Parses a Prometheus duration string into a time extent.
///
/// This function supports the full Prometheus unit set (`ms`, `s`, `m`, `h`,
/// `d`, `w`, `y`) and compound durations such as `1h30m`. It matches the
/// conformance harness' `parse_duration_ms`. Empty, negative, or unparseable
/// input is a hard error.
pub(crate) fn parse_duration(duration: &str) -> Result<Time, PromqlError> {
    let src = duration.trim();
    if src.is_empty() {
        return Err(PromqlError::Exec("empty duration".into()));
    }
    if src == "0" {
        return Ok(Time::ZERO);
    }
    if src.starts_with('-') {
        return Err(PromqlError::Exec(format!("negative duration `{src}`")));
    }

    let mut total_ms = 0_i64;
    let mut index = 0;
    let bytes = src.as_bytes();

    while index < bytes.len() {
        let start = index;
        while index < bytes.len() && bytes[index].is_ascii_digit() {
            index += 1;
        }
        if start == index {
            return Err(PromqlError::Exec(format!("invalid duration `{src}`")));
        }
        let amount = src[start..index]
            .parse::<i64>()
            .map_err(|err| PromqlError::Exec(format!("invalid duration amount `{src}`: {err}")))?;
        let unit_start = index;
        while index < bytes.len() && bytes[index].is_ascii_alphabetic() {
            index += 1;
        }
        let unit = &src[unit_start..index];
        let multiplier = match unit {
            "ms" => 1,
            "s" => 1_000,
            "m" => 60_000,
            "h" => 3_600_000,
            "d" => 86_400_000,
            "w" => 604_800_000,
            "y" => 31_536_000_000,
            _ => return Err(PromqlError::Exec(format!("invalid duration unit `{unit}`"))),
        };
        total_ms += amount
            .checked_mul(multiplier)
            .ok_or_else(|| PromqlError::Exec(format!("duration overflow `{src}`")))?;
    }

    Ok(Time::from_millis(total_ms))
}
