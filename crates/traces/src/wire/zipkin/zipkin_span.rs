use super::*;

#[derive(Deserialize)]
pub(crate) struct ZipkinSpan {
    #[serde(rename = "traceId")]
    pub(crate) trace_id: String,
    pub(crate) id: String,
    #[serde(rename = "parentId")]
    pub(crate) parent_id: Option<String>,
    #[serde(default)]
    pub(crate) name: String,
    #[serde(default)]
    pub(crate) timestamp: i64,
    #[serde(default)]
    pub(crate) duration: i64,
    pub(crate) kind: Option<String>,
    #[serde(rename = "localEndpoint")]
    pub(crate) local_endpoint: Option<ZipkinEndpoint>,
    #[serde(rename = "remoteEndpoint")]
    pub(crate) remote_endpoint: Option<ZipkinEndpoint>,
    #[serde(default)]
    pub(crate) tags: BTreeMap<String, String>,
    #[serde(default)]
    pub(crate) annotations: Vec<ZipkinAnnotation>,
}
