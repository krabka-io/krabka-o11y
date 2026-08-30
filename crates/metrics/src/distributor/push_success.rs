use super::{IntoResponse, Response, StatusCode, WrittenCounts, written_counts_response};

pub(crate) enum PushSuccess {
    Ok,
    Accepted { counts: Option<WrittenCounts> },
    NoContent { counts: Option<WrittenCounts> },
}

impl IntoResponse for PushSuccess {
    fn into_response(self) -> Response {
        match self {
            Self::Ok => StatusCode::OK.into_response(),
            Self::Accepted { counts: None } => StatusCode::ACCEPTED.into_response(),
            Self::Accepted {
                counts: Some(counts),
            } => written_counts_response(StatusCode::ACCEPTED, counts),
            Self::NoContent { counts: None } => StatusCode::NO_CONTENT.into_response(),
            Self::NoContent {
                counts: Some(counts),
            } => written_counts_response(StatusCode::NO_CONTENT, counts),
        }
    }
}
