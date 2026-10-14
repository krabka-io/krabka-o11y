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
        struct DirectionCase {
            input: Option<&'static str>,
            expected: LokiDirection,
        }
        for case in [
            DirectionCase {
                input: None,
                expected: LokiDirection::Backward,
            },
            DirectionCase {
                input: Some(""),
                expected: LokiDirection::Backward,
            },
            DirectionCase {
                input: Some("backward"),
                expected: LokiDirection::Backward,
            },
            DirectionCase {
                input: Some("BACKWARD"),
                expected: LokiDirection::Backward,
            },
            DirectionCase {
                input: Some("BaCkWaRd"),
                expected: LokiDirection::Backward,
            },
            DirectionCase {
                input: Some("forward"),
                expected: LokiDirection::Forward,
            },
            DirectionCase {
                input: Some("FORWARD"),
                expected: LokiDirection::Forward,
            },
            DirectionCase {
                input: Some("FoRwArD"),
                expected: LokiDirection::Forward,
            },
        ] {
            assert!(loki_direction(case.input).unwrap() == case.expected);
        }
        for input in ["sideways", "forward ", " backward", "0", " "] {
            assert!(matches!(
                loki_direction(Some(input)),
                Err(HttpQueryError::InvalidDirection(value)) if value == input
            ));
        }
    }
}
