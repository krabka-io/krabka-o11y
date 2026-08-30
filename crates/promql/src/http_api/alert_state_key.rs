use super::BTreeMap;

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct AlertStateKey {
    pub(crate) tenant: String,
    pub(crate) rule_id: String,
    pub(crate) labels: BTreeMap<String, String>,
}
