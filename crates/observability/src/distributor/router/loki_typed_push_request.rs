use super::{Deserialize, LokiPushStream};

#[derive(Debug, Deserialize)]
pub(crate) struct LokiTypedPushRequest {
    pub(crate) streams: Vec<LokiPushStream>,
}
