use std::{collections::BTreeSet, sync::LazyLock};

use regex::Regex;

static QUERY_CALL: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"\bquery\s+("(?:\\.|[^"\\])*"|`[^`]*`)"#).expect("valid query-call regex")
});

pub(crate) fn alert_template_queries<'a>(
    templates: impl Iterator<Item = &'a String>,
) -> BTreeSet<String> {
    templates
        .flat_map(|template| QUERY_CALL.captures_iter(template))
        .filter_map(|capture| {
            let quoted = capture.get(1)?.as_str();
            if let Some(query) = quoted.strip_prefix('`').and_then(|s| s.strip_suffix('`')) {
                Some(query.to_string())
            } else {
                serde_json::from_str(quoted).ok()
            }
        })
        .collect()
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
