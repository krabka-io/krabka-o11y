pub(crate) fn expected_logql_token(message: &str) -> String {
    match message {
        "expected '\"'" | "expected closing quote" => "STRING".to_string(),
        "expected label matcher operator" => "ASSIGN, EQ, NEQ, RE, NRE".to_string(),
        "expected label name" => "IDENTIFIER".to_string(),
        "expected end of query" => "$end".to_string(),
        _ => message
            .strip_prefix("expected ")
            .unwrap_or(message)
            .to_string(),
    }
}
