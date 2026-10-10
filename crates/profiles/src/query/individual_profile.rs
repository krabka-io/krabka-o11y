/// One individually stored profile: its `__profile_id__` label value, and the
/// labels each of its exemplars carries.
#[derive(Clone, Copy)]
pub(crate) struct IndividualProfile<'a> {
    pub(crate) profile_id: &'a str,
    pub(crate) labels: &'a [(String, String)],
}
