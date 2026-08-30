use super::{PromqlError, RulerWalError};

impl From<RulerWalError> for PromqlError {
    fn from(error: RulerWalError) -> Self {
        Self::Exec(error.to_string())
    }
}
