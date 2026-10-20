use super::{
    DistributorState, IntoResponse, Response, StatusCode, TenantId, WrittenCounts,
    written_counts_response,
};

pub(crate) enum PushSuccess {
    Ok { counts: Option<WrittenCounts> },
    Accepted { counts: Option<WrittenCounts> },
    NoContent { counts: Option<WrittenCounts> },
}

pub(crate) struct PushOutcome {
    pub(crate) success: PushSuccess,
    pub(crate) items: u64,
}

pub(crate) fn series_push_result(
    state: &DistributorState,
    tenant: &TenantId,
    items: u64,
    appended: bool,
) -> PushOutcome {
    let success = if appended {
        if let Some(metrics) = &state.metrics {
            metrics.record_ingest_series(tenant.as_str(), items);
        }
        PushSuccess::NoContent { counts: None }
    } else {
        PushSuccess::Accepted { counts: None }
    };
    PushOutcome { success, items }
}

impl IntoResponse for PushSuccess {
    fn into_response(self) -> Response {
        match self {
            Self::Ok { counts: None } => StatusCode::OK.into_response(),
            Self::Ok {
                counts: Some(counts),
            } => written_counts_response(StatusCode::OK, counts),
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
