use super::*;

pub(crate) fn parse_cases(file: &str, contents: &str) -> Vec<Case> {
    contents
        .split("\n---")
        .enumerate()
        .filter_map(|(idx, block)| {
            let mut case = Case {
                name: format!("{file}#{}", idx + 1),
                kind: "search".into(),
                ..Case::default()
            };
            for line in block.lines().map(str::trim) {
                if line.starts_with('#') {
                    continue;
                }
                let Some((key, value)) = line.split_once(':') else {
                    continue;
                };
                let value = value.trim();
                match key.trim() {
                    "name" => case.name = format!("{file}:{value}"),
                    "kind" => case.kind = value.to_string(),
                    "query" => case.query = Some(value.to_string()),
                    "trace_id" => case.trace_id = Some(parse_field(&case.name, "trace_id", value)),
                    "expect_trace_ids" => case.expect_trace_ids = Some(value.to_string()),
                    "expect_span_ids" => case.expect_span_ids = Some(value.to_string()),
                    "expect_series_count" => {
                        case.expect_series_count =
                            Some(parse_field(&case.name, "expect_series_count", value));
                    }
                    "expect_span_count" => {
                        case.expect_span_count =
                            Some(parse_field(&case.name, "expect_span_count", value));
                    }
                    // An unrecognised key is a typo in the corpus, not an
                    // optional extra. Silently ignoring it would drop the
                    // expectation it was meant to state and leave the case
                    // passing on a weaker assertion than its author wrote.
                    other => panic!("{}: unknown case key `{other}`", case.name),
                }
            }
            (!block.trim().is_empty()).then_some(case)
        })
        .collect()
}
