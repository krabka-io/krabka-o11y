use super::{ProfileError, Time, TimeExt, days, hours, minutes, secs};

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
