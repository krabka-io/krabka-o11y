use super::EncodeLabelSet;

/// Query-shape label: `type="instant"` or `type="range"`.
///
/// The label separates the engine-eval latency histogram and the eval-error
/// counter by the kind of `PromQL` query, and not by the HTTP route. For
/// example, `query` and a remote-read fanned instant query both have
/// `type="instant"`.
///
/// The field is the raw identifier `r#type`. The `EncodeLabelSet` derive maps a
/// keyword-raw ident back to its bare form, so the field encodes as the label
/// key `type`. The derive of this crate supports only `flatten`, not a `rename`
/// attribute.
#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct QueryTypeLabel {
    pub r#type: String,
}
