use super::*;

#[derive(Deserialize)]
pub(crate) struct ZipkinEndpoint {
    #[serde(rename = "serviceName")]
    pub(crate) service_name: Option<String>,
}
