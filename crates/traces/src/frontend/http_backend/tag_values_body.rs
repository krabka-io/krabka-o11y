use super::TypedValueJson;

/// The `/api/v2/search/tag/{tag}/values` body:
/// `{ tagValues: [{ type, value }], metrics }`.
#[derive(Clone, Debug, serde::Deserialize)]
pub(crate) struct TagValuesBody {
    #[serde(rename = "tagValues", default)]
    pub(crate) tag_values: Vec<TypedValueJson>,
    #[serde(default)]
    pub(crate) metrics: crate::frontend::wire::Metrics,
}

impl TagValuesBody {
    pub(crate) fn into_typed_values(self) -> Vec<krabka_traceql::TypedValue> {
        self.tag_values
            .into_iter()
            .map(|v| krabka_traceql::TypedValue {
                type_: v.type_,
                value: v.value,
            })
            .collect()
    }
}
