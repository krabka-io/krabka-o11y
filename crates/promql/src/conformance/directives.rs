use super::*;

pub(crate) fn parse_error(line: Line<'_>, message: impl Into<String>) -> PromqlError {
    PromqlError::Parse(format!("line {}: {}", line.number, message.into()))
}

pub(crate) fn parse_expect_directive(directive: &str, line: Line<'_>) -> Result<ExpectDirective> {
    let directive = directive.trim();
    match directive {
        "no_warn" => {
            return Ok(ExpectDirective::Annotation(AnnotationExpect::NoWarn));
        }
        "no_info" => {
            return Ok(ExpectDirective::Annotation(AnnotationExpect::NoInfo));
        }
        "warn" => {
            return Ok(ExpectDirective::Annotation(AnnotationExpect::AnyWarn));
        }
        "info" => {
            return Ok(ExpectDirective::Annotation(AnnotationExpect::AnyInfo));
        }
        "ordered" => return Ok(ExpectDirective::Ordered),
        _ => {}
    }
    if let Some(message) = directive.strip_prefix("warn msg:") {
        return Ok(ExpectDirective::Annotation(AnnotationExpect::WarnMsg(
            message.trim().to_string(),
        )));
    }
    if let Some(message) = directive.strip_prefix("info msg:") {
        return Ok(ExpectDirective::Annotation(AnnotationExpect::InfoMsg(
            message.trim().to_string(),
        )));
    }
    Err(parse_error(
        line,
        format!("unsupported expect directive `{directive}`"),
    ))
}

pub(crate) fn parse_expect_string(src: &str, line: Line<'_>) -> Result<String> {
    let src = src.trim();
    if let Some(value) = src
        .strip_prefix('`')
        .and_then(|value| value.strip_suffix('`'))
    {
        return Ok(value.to_string());
    }
    let value = src
        .strip_prefix('"')
        .and_then(|value| value.strip_suffix('"'))
        .ok_or_else(|| parse_error(line, "expected quoted string assertion"))?;
    Ok(value.replace("\\\"", "\"").replace("\\\\", "\\"))
}

pub(crate) fn parse_histogram_repetition(
    token: &str,
    line: Line<'_>,
) -> Result<Option<HistogramRepetition>> {
    let Some(separator) = token.rfind('x') else {
        return Ok(None);
    };
    let literal = &token[..separator];
    let count = &token[separator + 1..];
    if !literal.starts_with("{{") || !literal.ends_with("}}") {
        return Ok(None);
    }
    let count = count.parse::<u32>().map_err(|err| {
        parse_error(
            line,
            format!("invalid expanding-point count `{count}`: {err}"),
        )
    })?;
    Ok(Some(HistogramRepetition {
        histogram: parse_histogram_literal(literal, line)?,
        count,
    }))
}

pub(crate) fn parse_optional_histogram_buckets(
    fields: &BTreeMap<&str, &str>,
    name: &str,
    line: Line<'_>,
) -> Result<Option<Vec<f64>>> {
    let Some(value) = fields.get(name) else {
        return Ok(None);
    };
    let bucket_values = value
        .strip_prefix('[')
        .and_then(|value| value.strip_suffix(']'))
        .ok_or_else(|| {
            parse_error(
                line,
                format!("histogram {name} must be enclosed in `[` and `]`"),
            )
        })?;
    if bucket_values.trim().is_empty() {
        return Ok(Some(Vec::new()));
    }
    let values = bucket_values
        .split_whitespace()
        .map(|bucket| parse_float(bucket, line))
        .collect::<Result<Vec<_>>>()?;
    Ok(Some(values))
}

pub(crate) fn parse_optional_histogram_reset_hint(
    fields: &BTreeMap<&str, &str>,
    line: Line<'_>,
) -> Result<ResetHint> {
    match fields.get("counter_reset_hint").copied() {
        None | Some("unknown") => Ok(ResetHint::Unknown),
        Some("reset") => Ok(ResetHint::Yes),
        Some("not_reset") => Ok(ResetHint::No),
        Some("gauge") => Ok(ResetHint::Gauge),
        Some(value) => Err(parse_error(
            line,
            format!("invalid histogram counter_reset_hint `{value}`"),
        )),
    }
}

pub(crate) fn parse_range_vector_directive(
    directive: &str,
    line: Line<'_>,
) -> Result<Option<RangeExpect>> {
    let Some(rest) = directive.trim().strip_prefix("range vector from ") else {
        return Ok(None);
    };
    let parts = split_once_whitespace(rest.trim(), line)?;
    let start = parts.head;
    let rest = parts.tail;
    let rest = rest
        .strip_prefix("to ")
        .ok_or_else(|| parse_error(line, "expected `to` in expect range vector"))?;
    let parts = split_once_whitespace(rest.trim(), line)?;
    let rest = parts.tail;
    let rest = rest
        .strip_prefix("step ")
        .ok_or_else(|| parse_error(line, "expected `step` in expect range vector"))?;
    let step = rest.trim();
    Ok(Some(RangeExpect {
        start_ms: parse_duration_ms(start, line)?,
        step: Time::from_millis(parse_duration_ms(step, line)?),
    }))
}

struct RepeatedOffset<'a> {
    base: &'a str,
    count: usize,
}

struct StartOffsetParts<'a> {
    start: &'a str,
    step: Option<&'a str>,
}

pub(crate) fn parse_start_offset_token(token: &str, line: Line<'_>) -> Result<Vec<Option<i64>>> {
    if token == "_" {
        return Ok(vec![None]);
    }
    if let Some(count) = token.strip_prefix("_x") {
        let count = count
            .parse::<usize>()
            .map_err(|_| parse_error(line, "invalid @st repeat count"))?;
        if count == 0 {
            return Err(parse_error(line, "invalid @st repeat count"));
        }
        return Ok(vec![None; count]);
    }
    let repeated = if let Some(parts) = token.rsplit_once('x') {
        let count = parts
            .1
            .parse::<usize>()
            .map_err(|_| parse_error(line, "invalid @st repeat count"))?;
        RepeatedOffset {
            base: parts.0,
            count,
        }
    } else {
        RepeatedOffset {
            base: token,
            count: 0,
        }
    };
    let split = repeated
        .base
        .char_indices()
        .skip(usize::from(repeated.base.starts_with(['+', '-'])))
        .find_map(|entry| matches!(entry.1, '+' | '-').then_some(entry.0));
    let parts = match split {
        Some(index) => StartOffsetParts {
            start: &repeated.base[..index],
            step: Some(&repeated.base[index..]),
        },
        None => StartOffsetParts {
            start: repeated.base,
            step: None,
        },
    };
    let duration = |text: &str| -> Result<i64> {
        let unsigned = text.trim_start_matches(['+', '-']);
        let value = parse_duration_ms(unsigned, line)?;
        Ok(if text.starts_with('-') { -value } else { value })
    };
    let start = duration(parts.start)?;
    let step = parts.step.map(duration).transpose()?.unwrap_or(0);
    (0..=repeated.count)
        .map(|index| {
            let offset = i64::try_from(index)
                .ok()
                .and_then(|index| step.checked_mul(index))
                .and_then(|delta| start.checked_add(delta))
                .ok_or_else(|| parse_error(line, "@st offset overflow"))?;
            Ok(Some(offset))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expands_signed_duration_offsets_and_missing_slots() {
        let line = Line {
            number: 1,
            raw: "",
            trimmed: "",
        };
        assert2::assert!(
            parse_start_offset_token("-30s-1mx2", line).unwrap()
                == vec![Some(-30_000), Some(-90_000), Some(-150_000)]
        );
        assert2::assert!(parse_start_offset_token("1msx2", line).unwrap() == vec![Some(1); 3]);
        assert2::assert!(parse_start_offset_token("_x2", line).unwrap() == vec![None; 2]);
        for invalid in ["_x0", "-30s-badx2", "bad", "1msxno"] {
            assert2::assert!(parse_start_offset_token(invalid, line).is_err());
        }
    }
}
