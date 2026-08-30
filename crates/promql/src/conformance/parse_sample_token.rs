use super::{
    Line, Result, SampleSpec, add_histogram_step, parse_error, parse_float,
    parse_histogram_expansion, parse_histogram_literal, parse_histogram_repetition,
};

pub(crate) fn parse_sample_token(token: &str, line: Line<'_>) -> Result<Vec<SampleSpec>> {
    if token == "_" {
        return Ok(vec![SampleSpec::Missing]);
    }
    if token == "stale" {
        return Ok(vec![SampleSpec::Stale]);
    }
    if let Some((start, step, count)) = parse_histogram_expansion(token, line)? {
        return Ok((0..=count)
            .map(|offset| SampleSpec::Histogram(add_histogram_step(&start, &step, offset)))
            .collect());
    }
    if let Some((histogram, count)) = parse_histogram_repetition(token, line)? {
        return Ok((0..=count)
            .map(|_| SampleSpec::Histogram(histogram.clone()))
            .collect());
    }
    if token.starts_with("{{") {
        return Ok(vec![SampleSpec::Histogram(parse_histogram_literal(
            token, line,
        )?)]);
    }

    if let Some((base, count)) = token.rsplit_once('x') {
        let count = count.parse::<u32>().map_err(|err| {
            parse_error(
                line,
                format!("invalid expanding-point count `{count}`: {err}"),
            )
        })?;
        let step_index = base
            .char_indices()
            .skip(usize::from(base.starts_with('+') || base.starts_with('-')))
            .find_map(|(index, ch)| matches!(ch, '+' | '-').then_some((index, ch)));
        let (start, step) = match step_index {
            Some(index) => {
                let (index, sign) = index;
                let (start, step) = base.split_at(index);
                let step = parse_float(&step[1..], line)?;
                let step = if sign == '-' { -step } else { step };
                (parse_float(start, line)?, step)
            }
            None => (parse_float(base, line)?, 0.0),
        };
        return Ok((0..=count)
            .map(|offset| SampleSpec::Value(start + step * f64::from(offset)))
            .collect());
    }

    Ok(vec![SampleSpec::Value(parse_float(token, line)?)])
}
