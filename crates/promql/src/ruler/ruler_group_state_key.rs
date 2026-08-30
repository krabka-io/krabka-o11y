#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct RulerGroupStateKey {
    pub(crate) tenant: String,
    pub(crate) namespace: String,
    pub(crate) group: String,
}
