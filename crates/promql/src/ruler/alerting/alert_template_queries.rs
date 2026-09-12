use std::collections::BTreeSet;

pub(crate) fn alert_template_queries<'a>(
    templates: impl Iterator<Item = &'a String>,
) -> BTreeSet<String> {
    let mut queries = BTreeSet::new();
    for template in templates {
        if let Ok(format) = krabka_logql::LineFormat::new(template) {
            queries.extend(format.query_calls());
        }
    }
    queries
}

#[cfg(test)]
mod tests {
    use super::alert_template_queries;

    #[test]
    fn finds_distinct_quoted_query_calls() {
        let templates = [
            r#"{{ query "up{job=\"api\"}" | first | value }}"#.to_string(),
            "{{ query `sum(rate(errors[5m]))` | first }}".to_string(),
            r#"{{ query "up{job=\"api\"}" }}"#.to_string(),
            r#"last query "ignored" failed"#.to_string(),
        ];

        assert_eq!(
            alert_template_queries(templates.iter()),
            ["sum(rate(errors[5m]))", "up{job=\"api\"}"]
                .into_iter()
                .map(str::to_string)
                .collect()
        );
    }
}
