pub(crate) fn jfr_method_name(
    class: Option<jfrs::reader::types::builtin::Class<'_>>,
    method_name: &str,
) -> String {
    if method_name.contains("::") {
        return method_name.to_string();
    }
    class
        .and_then(|class| class.name)
        .and_then(|name| name.string)
        .map_or_else(
            || method_name.to_string(),
            |class_name| format!("{}.{}", class_name.replace('/', "."), method_name),
        )
}
