use super::{RulerWalError, PromqlError};

impl From<RulerWalError> for PromqlError {
    fn from(error: RulerWalError) -> Self {
        Self::Exec(error.to_string())
    }
}
