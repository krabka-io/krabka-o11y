use super::{Line, Result, parse_duration_ms, parse_error};

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
    let (base, count) = if let Some((base, count)) = token.rsplit_once('x') {
        let count = count
            .parse::<usize>()
            .map_err(|_| parse_error(line, "invalid @st repeat count"))?;
        (base, count)
    } else {
        (token, 0)
    };
    let split = base
        .char_indices()
        .skip(usize::from(base.starts_with(['+', '-'])))
        .find_map(|(index, ch)| matches!(ch, '+' | '-').then_some(index));
    let (start, step) = match split {
        Some(index) => (&base[..index], Some(&base[index..])),
        None => (base, None),
    };
    let duration = |text: &str| -> Result<i64> {
        let unsigned = text.trim_start_matches(['+', '-']);
        let value = parse_duration_ms(unsigned, line)?;
        Ok(if text.starts_with('-') { -value } else { value })
    };
    let start = duration(start)?;
    let step = step.map(duration).transpose()?.unwrap_or(0);
    (0..=count)
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
