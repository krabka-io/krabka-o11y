use super::{Cli, PromotedSpanAttr, RESOURCE_ATTR_PREFIX, parse_promoted_attr};

pub(crate) fn promoted_attrs_from_cli(cli: &Cli) -> Result<Vec<PromotedSpanAttr>, String> {
    let mut attrs = Vec::new();
    for spec in &cli.promote_resource_attrs {
        attrs.push(parse_promoted_attr(spec, Some(RESOURCE_ATTR_PREFIX))?);
    }
    for spec in &cli.promote_span_attrs {
        attrs.push(parse_promoted_attr(spec, None)?);
    }
    Ok(attrs)
}
