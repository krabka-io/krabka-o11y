use super::{Deserialize, Serialize};

/// OTLP status code.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum StatusCode {
    #[default]
    Unset,
    Ok,
    Error,
}

impl StatusCode {
    #[must_use]
    pub fn as_i32(self) -> i32 {
        self as i32
    }

    #[must_use]
    pub fn from_i32(v: i32) -> Self {
        match v {
            1 => Self::Ok,
            2 => Self::Error,
            _ => Self::Unset,
        }
    }
}
