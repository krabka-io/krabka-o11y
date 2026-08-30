pub(crate) fn loki_gzip_decode_error_text(source: &std::io::Error) -> String {
    let source = source.to_string();
    let message = match source.as_str() {
        "unexpected end of file" => "unexpected EOF",
        other => other,
    };
    format!("{message}\n")
}
