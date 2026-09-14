use super::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OtlpAttributeAction {
    IndexLabel,
    StructuredMetadata,
    Drop,
}
