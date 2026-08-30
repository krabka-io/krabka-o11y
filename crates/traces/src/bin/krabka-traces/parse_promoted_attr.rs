use super::*;

pub(crate) fn parse_promoted_attr(spec: &str, key_prefix: Option<&str>) -> Result<PromotedSpanAttr, String> {
    let (key, value_type) = spec.split_once(':').unwrap_or((spec, "string"));
    if key.is_empty() {
        return Err("promoted attribute key cannot be empty".into());
    }

    let key = format!("{}{}", key_prefix.unwrap_or_default(), key);
    match value_type {
        "string" | "str" => Ok(PromotedSpanAttr::string(key)),
        "int" | "i64" => Ok(PromotedSpanAttr::int(key)),
        "double" | "float" | "f64" => Ok(PromotedSpanAttr::double(key)),
        "bool" | "boolean" => Ok(PromotedSpanAttr::bool(key)),
        other => Err(format!(
            "unsupported promoted attribute type {other:?}; expected string, int, double, or bool"
        )),
    }
}
