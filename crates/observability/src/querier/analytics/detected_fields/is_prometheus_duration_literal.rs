use super::detected_duration_unit;

pub(crate) fn is_prometheus_duration_literal(value: &str) -> bool {
    let mut pos = 0;
    let mut parsed_chunk = false;
    let mut previous_unit_order = None;

    while pos < value.len() {
        let value_start = pos;
        while value.as_bytes().get(pos).is_some_and(u8::is_ascii_digit) {
            pos += 1;
        }
        if pos == value_start {
            return false;
        }

        let unit_start = pos;
        while value
            .as_bytes()
            .get(pos)
            .is_some_and(u8::is_ascii_alphabetic)
        {
            pos += 1;
        }
        let Some((unit_order, _)) = detected_duration_unit(&value[unit_start..pos]) else {
            return false;
        };
        if previous_unit_order.is_some_and(|previous| unit_order <= previous) {
            return false;
        }

        previous_unit_order = Some(unit_order);
        parsed_chunk = true;
    }

    parsed_chunk
}
