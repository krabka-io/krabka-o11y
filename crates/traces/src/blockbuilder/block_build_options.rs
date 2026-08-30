use super::PromotedSpanAttr;

pub(crate) struct BlockBuildOptions<'a> {
    pub(crate) object_key_prefix: &'a str,
    pub(crate) promoted_attrs: &'a [PromotedSpanAttr],
}
