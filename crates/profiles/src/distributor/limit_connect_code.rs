use super::Code;

pub(crate) fn limit_connect_code(err: &crate::limits::LimitError) -> Code {
    match err.connect_code() {
        "resource_exhausted" => Code::ResourceExhausted,
        "invalid_argument" => Code::InvalidArgument,
        _ => Code::Internal,
    }
}
