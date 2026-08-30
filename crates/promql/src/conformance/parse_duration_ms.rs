use super::*;

pub(crate) fn parse_duration_ms(src: &str, line: Line<'_>) -> Result<i64> {
    let src = src.trim();
    if src == "0" {
        return Ok(0);
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
            return Err(parse_error(line, format!("invalid duration `{src}`")));
        }
        let amount = src[start..index]
            .parse::<i64>()
            .map_err(|err| parse_error(line, format!("invalid duration amount `{src}`: {err}")))?;
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
            _ => return Err(parse_error(line, format!("invalid duration unit `{unit}`"))),
        };
        total_ms += amount * multiplier;
    }

    Ok(total_ms)
}
