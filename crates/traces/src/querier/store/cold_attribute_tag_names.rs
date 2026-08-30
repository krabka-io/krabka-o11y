use super::BTreeSet;

#[derive(Default)]
pub(crate) struct ColdAttributeTagNames {
    pub(crate) resource: BTreeSet<String>,
    pub(crate) span: BTreeSet<String>,
    pub(crate) event: BTreeSet<String>,
    pub(crate) link: BTreeSet<String>,
    pub(crate) instrumentation: BTreeSet<String>,
}
