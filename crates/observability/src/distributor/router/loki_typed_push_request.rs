use super::*;

#[derive(Debug, Deserialize)]
pub(crate) struct LokiTypedPushRequest {
    pub(crate) streams: Vec<LokiPushStream>,
}
