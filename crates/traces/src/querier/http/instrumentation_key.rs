#[derive(Clone, Debug, PartialEq)]
pub(crate) struct InstrumentationKey {
    pub(crate) name: String,
    pub(crate) version: String,
    pub(crate) attributes: Vec<krabka_traceql::Attribute>,
}
