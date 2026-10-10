/// Whether a row's attribute read includes its `resource.`-prefixed
/// attributes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ResourceAttrs {
    Include,
    Exclude,
}
