use super::HashMap;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct JfrLabels {
    pub(crate) global: Vec<(String, String)>,
    pub(crate) contexts: HashMap<i64, Vec<(String, String)>>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub(crate) struct LabelsSnapshot {
    #[prost(map = "int64, message", tag = "1")]
    pub contexts: HashMap<i64, LabelContext>,
    #[prost(map = "int64, string", tag = "2")]
    pub strings: HashMap<i64, String>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub(crate) struct LabelContext {
    #[prost(map = "int64, int64", tag = "1")]
    pub labels: HashMap<i64, i64>,
}
