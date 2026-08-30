pub(crate) fn indent_template_string(spaces: usize, value: &str) -> String {
    let prefix = " ".repeat(spaces);
    value
        .split('\n')
        .map(|line| format!("{prefix}{line}"))
        .collect::<Vec<_>>()
        .join("\n")
}
