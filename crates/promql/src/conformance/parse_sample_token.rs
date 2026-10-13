use super::{
    Line, Result, SampleSpec, add_histogram_step, parse_error, parse_float,
    parse_histogram_expansion, parse_histogram_literal, parse_histogram_repetition,
};

struct SampleRepetition<'a> {
    base: &'a str,
    count: u32,
}

struct ExpandedSampleValue {
    start: f64,
    step: f64,
}

pub(crate) fn parse_sample_token(token: &str, line: Line<'_>) -> Result<Vec<SampleSpec>> {
    if token == "_" {
        return Ok(vec![SampleSpec::Missing]);
    }
    if token == "stale" {
        return Ok(vec![SampleSpec::Stale]);
    }
    if let Some(expansion) = parse_histogram_expansion(token, line)? {
        return Ok((0..=expansion.count)
            .map(|offset| {
                SampleSpec::Histogram(add_histogram_step(
                    &expansion.start,
                    &expansion.step,
                    offset,
                ))
            })
            .collect());
    }
    if let Some(repetition) = parse_histogram_repetition(token, line)? {
        return Ok((0..=repetition.count)
            .map(|_| SampleSpec::Histogram(repetition.histogram.clone()))
            .collect());
    }
    if token.starts_with("{{") {
        return Ok(vec![SampleSpec::Histogram(parse_histogram_literal(
            token, line,
        )?)]);
    }

    if let Some(parts) = token.rsplit_once('x') {
        let count = parts.1.parse::<u32>().map_err(|err| {
            parse_error(
                line,
                format!("invalid expanding-point count `{}`: {err}", parts.1),
            )
        })?;
        let repetition = SampleRepetition {
            base: parts.0,
            count,
        };
        let base = repetition.base;
        let count = repetition.count;
        let step_index = base
            .char_indices()
            .skip(usize::from(base.starts_with('+') || base.starts_with('-')))
            .find_map(|entry| matches!(entry.1, '+' | '-').then_some(entry.0));
        let expanded = match step_index {
            Some(index) => {
                let parts = base.split_at(index);
                let parsed_step = parse_float(&parts.1[1..], line)?;
                let step = if base.as_bytes()[index] == b'-' {
                    -parsed_step
                } else {
                    parsed_step
                };
                ExpandedSampleValue {
                    start: parse_float(parts.0, line)?,
                    step,
                }
            }
            None => ExpandedSampleValue {
                start: parse_float(base, line)?,
                step: 0.0,
            },
        };
        return Ok((0..=count)
            .map(|offset| SampleSpec::Value(expanded.start + expanded.step * f64::from(offset)))
            .collect());
    }

    Ok(vec![SampleSpec::Value(parse_float(token, line)?)])
}
