use super::*;

#[derive(Debug, Deserialize)]
pub(crate) struct LokiPushRequest {
    #[serde(default)]
    pub(crate) streams: Option<Value>,
}
