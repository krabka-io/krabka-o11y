use super::*;

/// OTLP status code.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StatusCode {
    Unset,
    Ok,
    Error,
}

impl StatusCode {
    #[must_use]
    pub fn as_i32(self) -> i32 {
        match self {
            StatusCode::Unset => 0,
            StatusCode::Ok => 1,
            StatusCode::Error => 2,
        }
    }

    #[must_use]
    pub fn from_i32(v: i32) -> Self {
        match v {
            1 => StatusCode::Ok,
            2 => StatusCode::Error,
            _ => StatusCode::Unset,
        }
    }
}
