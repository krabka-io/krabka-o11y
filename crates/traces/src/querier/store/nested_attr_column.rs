use super::NestedAttrScope;

#[derive(Clone, Copy)]
pub(crate) struct NestedAttrColumn<'a> {
    pub(crate) scope: NestedAttrScope,
    pub(crate) key: &'a str,
}
