use krabka_blockstore::Labels;

use super::{DecodedSample, DecodedSeries, WireError};

pub(crate) fn decode_influx(
    body: &[u8],
    raw_query: Option<&str>,
    now_ms: i64,
) -> Result<Vec<DecodedSeries>, WireError> {
    let precision = raw_query
        .and_then(|query| {
            url::form_urlencoded::parse(query.as_bytes())
                .find(|(name, _)| name == "precision")
                .map(|(_, value)| value.into_owned())
        })
        .unwrap_or_else(|| "ns".into());
    let scale = timestamp_scale(&precision)?;
    let input = std::str::from_utf8(body)
        .map_err(|error| WireError::Invalid(format!("Influx body is not UTF-8: {error}")))?;
    let mut output = Vec::new();
    for (line_number, line) in input.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        parse_line(line, line_number + 1, scale, now_ms, &mut output)?;
    }
    Ok(output)
}

fn timestamp_scale(precision: &str) -> Result<i64, WireError> {
    match precision {
        "ns" | "n" => Ok(-1_000_000),
        "us" | "u" | "µ" => Ok(-1_000),
        "ms" => Ok(1),
        "s" => Ok(1_000),
        "m" => Ok(60_000),
        "h" => Ok(3_600_000),
        _ => Err(WireError::Invalid(format!(
            "precision supplied is not valid: {precision}"
        ))),
    }
}

fn parse_line(
    line: &str,
    line_number: usize,
    scale: i64,
    now_ms: i64,
    output: &mut Vec<DecodedSeries>,
) -> Result<(), WireError> {
    let first_space = find_unescaped(line, ' ', false)
        .ok_or_else(|| invalid_line(line_number, "missing field set"))?;
    let series = &line[..first_space];
    let rest = line[first_space + 1..].trim_start();
    let second_space = find_unescaped(rest, ' ', true);
    let (fields, timestamp) = second_space.map_or((rest, None), |index| {
        (&rest[..index], Some(rest[index + 1..].trim()))
    });
    let timestamp_ms = timestamp.map_or(Ok(now_ms), |timestamp| {
        let timestamp = timestamp
            .parse::<i64>()
            .map_err(|_| invalid_line(line_number, "invalid timestamp"))?;
        Ok(if scale < 0 {
            timestamp / scale.saturating_abs()
        } else {
            timestamp.saturating_mul(scale)
        })
    })?;

    let mut series_parts = split_unescaped(series, ',', false);
    let measurement = series_parts
        .next()
        .filter(|measurement| !measurement.is_empty())
        .ok_or_else(|| invalid_line(line_number, "missing measurement"))?;
    let measurement = sanitize_name(&unescape(measurement));
    let mut base_labels = Labels::new();
    base_labels.insert("__proxy_source__", "influx");
    for tag in series_parts {
        let (name, value) =
            split_pair(tag, '=', false).ok_or_else(|| invalid_line(line_number, "invalid tag"))?;
        let name = sanitize_name(&unescape(name));
        if matches!(name.as_str(), "__name__" | "__proxy_source__") {
            continue;
        }
        base_labels.insert(name, unescape(value));
    }

    for field in split_unescaped(fields, ',', true) {
        let (name, value) = split_pair(field, '=', true)
            .ok_or_else(|| invalid_line(line_number, "invalid field"))?;
        let Some(value) = field_value(value, line_number)? else {
            continue;
        };
        let field = sanitize_name(&unescape(name));
        let metric = if field == "value" {
            measurement.clone()
        } else {
            format!("{measurement}_{field}")
        };
        let mut labels = base_labels.clone();
        labels.insert("__name__", metric);
        output.push(DecodedSeries {
            labels,
            samples: vec![DecodedSample::new(timestamp_ms, value)],
            histograms: Vec::new(),
            exemplars: Vec::new(),
            metadata: None,
        });
    }
    Ok(())
}

fn field_value(value: &str, line_number: usize) -> Result<Option<f64>, WireError> {
    if value.starts_with('"') {
        return Ok(None);
    }
    if let Some(value) = value.strip_suffix('i') {
        return value
            .parse::<i64>()
            .map(|value| num_traits::ToPrimitive::to_f64(&value))
            .map_err(|_| invalid_line(line_number, "invalid integer field"));
    }
    if let Some(value) = value.strip_suffix('u') {
        return value
            .parse::<u64>()
            .map(|value| num_traits::ToPrimitive::to_f64(&value))
            .map_err(|_| invalid_line(line_number, "invalid unsigned integer field"));
    }
    match value {
        "t" | "T" | "true" | "TRUE" | "True" => return Ok(Some(1.0)),
        "f" | "F" | "false" | "FALSE" | "False" => return Ok(Some(0.0)),
        _ => {}
    }
    value
        .parse::<f64>()
        .map(Some)
        .map_err(|_| invalid_line(line_number, "invalid numeric field"))
}

fn sanitize_name(name: &str) -> String {
    let mut output = String::with_capacity(name.len() + 1);
    if name.as_bytes().first().is_some_and(u8::is_ascii_digit) {
        output.push('_');
    }
    output.extend(name.chars().map(|character| {
        if character == '_' || character.is_ascii_alphanumeric() {
            character
        } else {
            '_'
        }
    }));
    output
}

fn unescape(value: &str) -> String {
    let mut output = String::with_capacity(value.len());
    let mut escaped = false;
    for character in value.chars() {
        if escaped {
            output.push(character);
            escaped = false;
        } else if character == '\\' {
            escaped = true;
        } else {
            output.push(character);
        }
    }
    if escaped {
        output.push('\\');
    }
    output
}

fn split_pair(value: &str, delimiter: char, quotes: bool) -> Option<(&str, &str)> {
    find_unescaped(value, delimiter, quotes).map(|index| (&value[..index], &value[index + 1..]))
}

fn split_unescaped(value: &str, delimiter: char, quotes: bool) -> SplitUnescaped<'_> {
    SplitUnescaped {
        value,
        delimiter,
        quotes,
    }
}

fn find_unescaped(value: &str, delimiter: char, honor_quotes: bool) -> Option<usize> {
    let mut escaped = false;
    let mut quoted = false;
    for (index, character) in value.char_indices() {
        if escaped {
            escaped = false;
        } else if character == '\\' {
            escaped = true;
        } else if honor_quotes && character == '"' {
            quoted = !quoted;
        } else if !quoted && character == delimiter {
            return Some(index);
        }
    }
    None
}

struct SplitUnescaped<'a> {
    value: &'a str,
    delimiter: char,
    quotes: bool,
}

impl<'a> Iterator for SplitUnescaped<'a> {
    type Item = &'a str;

    fn next(&mut self) -> Option<Self::Item> {
        if self.value.is_empty() {
            return None;
        }
        if let Some(index) = find_unescaped(self.value, self.delimiter, self.quotes) {
            let item = &self.value[..index];
            self.value = &self.value[index + 1..];
            Some(item)
        } else {
            Some(std::mem::take(&mut self.value))
        }
    }
}

fn invalid_line(line_number: usize, reason: &str) -> WireError {
    WireError::Invalid(format!("Influx line {line_number}: {reason}"))
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn influx_fields_tags_escapes_and_precision_match_mimir_mapping() {
        let decoded = decode_influx(
            br#"cpu\ load,host=a\ b value=1.5,count=2i,live=true,note="skip" 1234000"#,
            Some("precision=us"),
            0,
        )
        .unwrap();

        assert!(decoded.len() == 3);
        assert!(decoded[0].labels.get("__name__") == Some("cpu_load"));
        assert!(decoded[0].labels.get("host") == Some("a b"));
        assert!(decoded[0].labels.get("__proxy_source__") == Some("influx"));
        assert!(decoded[0].samples == vec![DecodedSample::new(1_234, 1.5)]);
        assert!(decoded[1].labels.get("__name__") == Some("cpu_load_count"));
        assert!(decoded[2].labels.get("__name__") == Some("cpu_load_live"));
    }

    #[test]
    fn influx_rejects_an_invalid_precision_and_line() {
        assert!(decode_influx(b"cpu value=1", Some("precision=fortnight"), 0).is_err());
        assert!(decode_influx(b"cpu", None, 0).is_err());
    }
}
