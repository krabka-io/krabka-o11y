use super::HttpQueryError;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum LokiDirection {
    Forward,
    Backward,
}

pub(crate) fn loki_direction(direction: Option<&str>) -> Result<LokiDirection, HttpQueryError> {
    let direction = direction.unwrap_or("");
    if direction.is_empty() || direction.eq_ignore_ascii_case("backward") {
        Ok(LokiDirection::Backward)
    } else if direction.eq_ignore_ascii_case("forward") {
        Ok(LokiDirection::Forward)
    } else {
        Err(HttpQueryError::InvalidDirection(direction.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn direction_defaults_and_case_match_lokis_http_parser() {
        for (input, expected) in [
            (None, LokiDirection::Backward),
            (Some(""), LokiDirection::Backward),
            (Some("backward"), LokiDirection::Backward),
            (Some("BACKWARD"), LokiDirection::Backward),
            (Some("BaCkWaRd"), LokiDirection::Backward),
            (Some("forward"), LokiDirection::Forward),
            (Some("FORWARD"), LokiDirection::Forward),
            (Some("FoRwArD"), LokiDirection::Forward),
        ] {
            assert!(loki_direction(input).unwrap() == expected);
        }
        for input in ["sideways", "forward ", " backward", "0", " "] {
            assert!(matches!(
                loki_direction(Some(input)),
                Err(HttpQueryError::InvalidDirection(value)) if value == input
            ));
        }
    }
}
