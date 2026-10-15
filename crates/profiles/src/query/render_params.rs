use super::*;

/// The `now-<offset>` lookback of Pyroscope's `/render` `from`/`until` params.
///
/// The grammar is Pyroscope's, not `krabka-units`': a bare integer followed by
/// exactly one of `s`, `m`, `h`, or `d`. The result is an extent, so it is a
/// [`Time`]. The instant that it resolves against stays epoch milliseconds at
/// the call site.
pub(crate) fn parse_render_offset(value: &str) -> Result<Time, ProfileError> {
    let Some(unit_character) = value.chars().next_back() else {
        return Err(ProfileError::Plan(format!(
            "invalid render relative duration {value:?}"
        )));
    };
    let number_end = value.len() - unit_character.len_utf8();
    let number = &value[..number_end];
    let amount = number.parse::<i64>().map_err(|err| {
        ProfileError::Plan(format!("invalid render relative duration {value:?}: {err}"))
    })?;
    let unit = match unit_character {
        's' => secs(1),
        'm' => minutes(1),
        'h' => hours(1),
        'd' => days(1),
        _ => {
            return Err(ProfileError::Plan(format!(
                "invalid render relative duration unit {unit_character:?}"
            )));
        }
    };
    // The offset resolves against an epoch-millisecond instant, so it is scaled
    // in whole milliseconds and an offset too large to express there stays an
    // error rather than saturating into a silently different lookback.
    amount
        .checked_mul(unit.millis_i64())
        .map(Time::from_millis)
        .ok_or_else(|| ProfileError::Plan(format!("render relative duration overflows: {value}")))
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct ParsedRenderQuery {
    pub(crate) profile_type: String,
    pub(crate) selector: String,
}

pub(crate) fn parse_render_query(query: &str) -> Result<ParsedRenderQuery, ProfileError> {
    let trimmed = query.trim();
    let Some(open) = trimmed.find('{') else {
        if trimmed.is_empty() {
            return Err(ProfileError::Plan("missing query".to_string()));
        }
        return Ok(ParsedRenderQuery {
            profile_type: trimmed.to_string(),
            selector: "{}".to_string(),
        });
    };
    let profile_type = trimmed[..open].trim();
    let selector = &trimmed[open..];
    if profile_type.is_empty() {
        return Err(ProfileError::Plan("missing profile type".to_string()));
    }
    parse_label_selector(selector)?;
    Ok(ParsedRenderQuery {
        profile_type: profile_type.to_string(),
        selector: selector.to_string(),
    })
}

pub(crate) fn parse_render_time_param(
    value: Option<&str>,
    now_ms: NowMs,
    default: DefaultMs,
) -> Result<i64, ProfileError> {
    let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(default.0);
    };
    if value == "now" {
        return Ok(now_ms.0);
    }
    if let Some(offset) = value.strip_prefix("now-") {
        // The offset is an extent; `now` and the resolved bound are instants, so
        // the subtraction happens in epoch milliseconds.
        let resolved = now_ms.0 - parse_render_offset(offset)?.millis_i64();
        return reject_negative_render_time(resolved, value);
    }
    let numeric = value
        .parse::<i64>()
        .map_err(|err| ProfileError::Plan(format!("invalid render time {value:?}: {err}")))?;
    reject_negative_render_time(normalize_render_unix_time(numeric), value)
}
