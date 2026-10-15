/// Identity of one alert instance in the in-memory ruler state.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct AlertStateKey {
    pub(crate) tenant: String,
    pub(crate) rule_id: String,
    pub(crate) labels: crate::PromqlLabels,
}

/// Identity of one group in the in-memory ruler state.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct RulerGroupStateKey {
    pub(crate) tenant: String,
    pub(crate) namespace: String,
    pub(crate) group: String,
}
