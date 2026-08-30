use super::*;

#[derive(Clone, Copy, Debug)]
pub(crate) struct RuleRenderOptions {
    pub(crate) type_filter: RuleTypeFilter,
    pub(crate) exclude_alerts: bool,
}
