use super::TenantId;

#[derive(Clone, Copy, Debug)]
pub(crate) struct QueryTarget<'a> {
    pub tenant: &'a TenantId,
    pub profile_type: &'a str,
    pub label_selector: &'a str,
}

impl<'a> QueryTarget<'a> {
    pub(crate) const fn new(
        tenant: &'a TenantId,
        profile_type: &'a str,
        label_selector: &'a str,
    ) -> Self {
        Self {
            tenant,
            profile_type,
            label_selector,
        }
    }
}
