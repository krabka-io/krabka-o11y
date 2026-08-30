use super::*;

#[derive(Clone, Copy)]
pub(crate) enum LokiDirection {
    Forward,
    Backward,
}

pub(crate) fn loki_direction(direction: Option<&str>) -> Result<LokiDirection, HttpQueryError> {
    match direction {
        None | Some("backward") => Ok(LokiDirection::Backward),
        Some("forward") => Ok(LokiDirection::Forward),
        Some(value) => Err(HttpQueryError::InvalidDirection(value.to_string())),
    }
}
