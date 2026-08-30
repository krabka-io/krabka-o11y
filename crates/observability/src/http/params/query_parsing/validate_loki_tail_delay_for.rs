use super::*;

pub(crate) fn validate_loki_tail_delay_for(delay_for: i64) -> Result<(), HttpQueryError> {
    if !(0..=LOKI_MAX_TAIL_DELAY.nanos_i64()).contains(&delay_for) {
        return Err(HttpQueryError::TailDelayForTooLarge);
    }

    Ok(())
}
