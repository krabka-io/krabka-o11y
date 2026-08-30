use super::*;

#[derive(Debug, Deserialize)]
pub(crate) struct LokiPushStream {
    #[serde(default)]
    pub(crate) stream: Option<Labels>,
    #[serde(default)]
    pub(crate) values: Option<Vec<Value>>,
}
