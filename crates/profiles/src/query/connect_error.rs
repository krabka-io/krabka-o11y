use super::*;

pub(crate) fn connect_error(err: ProfileError) -> ConnectError {
    let code = match &err {
        ProfileError::Decode(_) | ProfileError::Plan(_) | ProfileError::Unsupported(_) => {
            Code::InvalidArgument
        }
        ProfileError::Exec(_) | ProfileError::Store(_) | ProfileError::Symbolize(_) => {
            Code::Internal
        }
    };
    let message = err.to_string();
    drop(err);
    ConnectError::new(code, message)
}
