use super::{TemplateRuntimeValue, template_value_is_collection};

pub(crate) struct TemplateCollectionFirstArgs<'a> {
    pub collection: &'a TemplateRuntimeValue,
    pub args: &'a [TemplateRuntimeValue],
}

pub(crate) fn template_collection_first_args(
    args: &[TemplateRuntimeValue],
) -> Option<TemplateCollectionFirstArgs<'_>> {
    let first = args.first()?;
    let rest = &args[1..];
    if template_value_is_collection(first) {
        return Some(TemplateCollectionFirstArgs {
            collection: first,
            args: rest,
        });
    }
    let last = args.last()?;
    template_value_is_collection(last).then_some(TemplateCollectionFirstArgs {
        collection: last,
        args: &args[..args.len() - 1],
    })
}
