use super::*;

pub(crate) fn template_collection_first_args(
    args: &[TemplateRuntimeValue],
) -> Option<(&TemplateRuntimeValue, &[TemplateRuntimeValue])> {
    let (first, rest) = args.split_first()?;
    if template_value_is_collection(first) {
        return Some((first, rest));
    }
    let (last, rest) = args.split_last()?;
    template_value_is_collection(last).then_some((last, rest))
}
