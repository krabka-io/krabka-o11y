tokio::task_local! {
    /// Original query text used to attach Prometheus-compatible source positions.
    pub(crate) static ANNOTATION_SOURCE: String;
}

pub(crate) fn with_source_position(message: String) -> String {
    ANNOTATION_SOURCE
        .try_with(|query| {
            annotation_offset(query, &message).map_or(message.clone(), |offset| {
                let before = &query[..offset];
                let line = before.bytes().filter(|byte| *byte == b'\n').count() + 1;
                let column = before
                    .rfind('\n')
                    .map_or(before.len(), |newline| before.len() - newline - 1)
                    + 1;
                format!("{message} ({line}:{column})")
            })
        })
        .unwrap_or(message)
}

fn annotation_offset(query: &str, message: &str) -> Option<usize> {
    // ponytail: promql-parser drops spans; delete this text resolver when its AST exposes them.
    if message.contains("ratio value should be between -1 and 1") {
        return None;
    }
    if message.starts_with("PromQL info: metric might not be a counter") {
        return counter_argument_offset(query, quoted_tail(message));
    }
    if message.starts_with("PromQL info: incompatible sample types encountered") {
        return first_expression_byte(query);
    }
    if let Some(rest) = message.strip_prefix("PromQL info: ignored histogram in ")
        && let Some(operation) = rest.strip_suffix(" aggregation")
    {
        return if matches!(operation, "topk" | "bottomk") {
            find_word(query, operation, 0)
        } else {
            aggregate_argument(query, operation, usize::from(operation == "quantile"))
        };
    }
    if message.contains("quantile value should be between 0 and 1") {
        return ["histogram_quantile", "quantile_over_time"]
            .into_iter()
            .find_map(|name| call_argument(query, name, 0))
            .or_else(|| aggregate_argument(query, "quantile", 0));
    }
    if message.contains("input to histogram_quantile has NaN observations")
        || message.contains("input to histogram_fraction has NaN observations")
    {
        let name = if message.contains("histogram_fraction") {
            "histogram_fraction"
        } else {
            "histogram_quantile"
        };
        return call_argument(query, name, 0);
    }
    if message.contains("input to histogram_quantile needed to be fixed")
        || message.contains("bucket label \"le\"")
    {
        return call_argument(query, "histogram_quantile", 1);
    }
    if message.contains("mix of classic and native histograms") {
        return call_argument(query, "histogram_quantile", 1)
            .or_else(|| call_argument(query, "histogram_fraction", 2));
    }
    if message.contains("mix of histograms with exponential and custom buckets")
        && ["rate", "increase"]
            .into_iter()
            .any(|name| find_word(query, name, 0).is_some())
    {
        return ["rate", "increase"]
            .into_iter()
            .find_map(|name| call_argument(query, name, 0));
    }
    if message.contains("mismatched custom buckets were reconciled during aggregation") {
        return histogram_aggregation_source(query);
    }
    if message.contains("mismatched custom buckets were reconciled during subtraction") {
        return ["rate", "increase", "delta", "irate", "idelta"]
            .into_iter()
            .find_map(|name| call_argument(query, name, 0))
            .or_else(|| first_expression_byte(query));
    }
    if message.contains("mismatched custom buckets were reconciled during addition") {
        return first_expression_byte(query);
    }
    if message.contains("conflicting counter resets during histogram aggregation") {
        return histogram_aggregation_source(query);
    }
    if message.contains("ignored histograms in a range containing both floats and histograms")
        && query.contains("quantile_over_time")
    {
        return call_argument(query, "quantile_over_time", 0);
    }
    if let Some(metric) = quoted_tail(message) {
        return rfind_identifier(query, metric);
    }
    if message.ends_with("for aggregation") {
        return ["sum", "avg"]
            .into_iter()
            .find_map(|name| aggregate_argument(query, name, 0));
    }
    first_expression_byte(query)
}

fn histogram_aggregation_source(query: &str) -> Option<usize> {
    ["sum_over_time", "avg_over_time"]
        .into_iter()
        .find_map(|name| call_argument(query, name, 0))
        .or_else(|| aggregate_argument(query, "sum", 0))
        .or_else(|| aggregate_argument(query, "avg", 0))
}

fn quoted_tail(message: &str) -> Option<&str> {
    let end = message.strip_suffix('"')?;
    let delimiter = end.rfind('"')?;
    Some(&end[delimiter + 1..])
}

fn first_expression_byte(query: &str) -> Option<usize> {
    query.find(|character: char| !character.is_whitespace())
}

fn rfind_identifier(query: &str, identifier: &str) -> Option<usize> {
    let mut from = 0;
    let mut found = None;
    while let Some(relative) = query[from..].find(identifier) {
        let offset = from + relative;
        let before = query[..offset].chars().next_back();
        let after = query[offset + identifier.len()..].chars().next();
        if !before.is_some_and(is_identifier_char) && !after.is_some_and(is_identifier_char) {
            found = Some(offset);
        }
        from = offset + identifier.len();
    }
    found
}

fn is_identifier_char(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || matches!(ch, '_' | ':' | '.')
}

fn counter_argument_offset(query: &str, metric: Option<&str>) -> Option<usize> {
    let mut candidates = Vec::new();
    for name in ["rate", "increase"] {
        let mut from = 0;
        while let Some(start) = find_word(query, name, from) {
            from = start + name.len();
            let open = skip_space(query, from);
            if inside_string(query, start) || !query[open..].starts_with('(') {
                continue;
            }
            let Some(close) = matching_paren(query, open) else {
                continue;
            };
            let argument = &query[open + 1..close];
            let offset = if rate_argument_is_subquery(argument) {
                // Prometheus replaces a subquery argument with a materialized
                // selector whose source range has the default zero offset.
                0
            } else {
                argument_at(query, open, 0)?
            };
            candidates.push(CounterArgumentCandidate {
                start,
                offset,
                matches_metric: metric
                    .is_some_and(|metric| rfind_identifier(argument, metric).is_some()),
            });
        }
    }
    candidates.sort_by_key(|candidate| candidate.start);
    candidates
        .iter()
        .rev()
        .find(|candidate| candidate.matches_metric)
        .or_else(|| candidates.last())
        .map(|candidate| candidate.offset)
}

struct CounterArgumentCandidate {
    start: usize,
    offset: usize,
    matches_metric: bool,
}

fn inside_string(query: &str, offset: usize) -> bool {
    let mut quote = None;
    let mut escaped = false;
    for ch in query[..offset].chars() {
        if let Some(delimiter) = quote {
            if escaped {
                escaped = false;
            } else if ch == '\\' && delimiter != '`' {
                escaped = true;
            } else if ch == delimiter {
                quote = None;
            }
        } else if matches!(ch, '"' | '\'' | '`') {
            quote = Some(ch);
        }
    }
    quote.is_some()
}

fn rate_argument_is_subquery(argument: &str) -> bool {
    let mut quoted = false;
    let mut escaped = false;
    let mut bracket_depth = 0_usize;
    argument.chars().any(|ch| {
        if quoted {
            escaped = ch == '\\' && !escaped;
            if ch == '"' && !escaped {
                quoted = false;
            }
            if ch != '\\' {
                escaped = false;
            }
            return false;
        }
        match ch {
            '"' => quoted = true,
            '[' => bracket_depth += 1,
            ']' => bracket_depth = bracket_depth.saturating_sub(1),
            ':' if bracket_depth > 0 => return true,
            _ => {}
        }
        false
    })
}

fn call_argument(query: &str, name: &str, index: usize) -> Option<usize> {
    let start = find_word(query, name, 0)?;
    let open = query[start + name.len()..].find('(')? + start + name.len();
    argument_at(query, open, index)
}

fn aggregate_argument(query: &str, name: &str, index: usize) -> Option<usize> {
    let start = find_word(query, name, 0)?;
    let mut cursor = start + name.len();
    cursor = skip_space(query, cursor);
    for modifier in ["by", "without"] {
        if query[cursor..].starts_with(modifier)
            && !query[cursor + modifier.len()..]
                .chars()
                .next()
                .is_some_and(is_identifier_char)
        {
            cursor = skip_space(query, cursor + modifier.len());
            let close = matching_paren(query, cursor)?;
            cursor = skip_space(query, close + 1);
            break;
        }
    }
    let open = query[cursor..].find('(')? + cursor;
    argument_at(query, open, index)
}

fn argument_at(query: &str, open: usize, wanted: usize) -> Option<usize> {
    let mut depth = 0_usize;
    let mut argument = 0_usize;
    let mut quoted = false;
    let mut escaped = false;
    let mut offset = open + 1;
    while offset < query.len() {
        let ch = query[offset..]
            .chars()
            .next()
            .expect("offset is in the query");
        if argument == wanted && depth == 0 && !ch.is_whitespace() {
            return Some(skip_grouping_parens(query, offset));
        }
        if quoted {
            escaped = ch == '\\' && !escaped;
            if ch == '"' && !escaped {
                quoted = false;
            }
            if ch != '\\' {
                escaped = false;
            }
            offset += ch.len_utf8();
            continue;
        }
        match ch {
            '"' => quoted = true,
            '(' | '[' | '{' => depth += 1,
            ')' if depth == 0 => return None,
            ')' | ']' | '}' => depth = depth.saturating_sub(1),
            ',' if depth == 0 => argument += 1,
            _ => {}
        }
        offset += ch.len_utf8();
    }
    None
}

fn skip_grouping_parens(query: &str, mut offset: usize) -> usize {
    loop {
        offset = skip_space(query, offset);
        if query[offset..].starts_with('(') {
            offset += 1;
        } else {
            return offset;
        }
    }
}

fn find_word(query: &str, word: &str, from: usize) -> Option<usize> {
    let mut cursor = from;
    while let Some(relative) = query[cursor..].find(word) {
        let offset = cursor + relative;
        let before = query[..offset].chars().next_back();
        let after = query[offset + word.len()..].chars().next();
        if !before.is_some_and(is_identifier_char) && !after.is_some_and(is_identifier_char) {
            return Some(offset);
        }
        cursor = offset + word.len();
    }
    None
}

fn skip_space(query: &str, mut offset: usize) -> usize {
    while let Some(ch) = query[offset..].chars().next()
        && ch.is_whitespace()
    {
        offset += ch.len_utf8();
    }
    offset
}

fn matching_paren(query: &str, open: usize) -> Option<usize> {
    if !query[open..].starts_with('(') {
        return None;
    }
    let mut depth = 0_usize;
    let mut quote = None;
    let mut escaped = false;
    let mut offset = open;
    while offset < query.len() {
        let ch = query[offset..]
            .chars()
            .next()
            .expect("offset is in the query");
        if let Some(delimiter) = quote {
            if escaped {
                escaped = false;
            } else if ch == '\\' && delimiter != '`' {
                escaped = true;
            } else if ch == delimiter {
                quote = None;
            }
            offset += ch.len_utf8();
            continue;
        }
        if matches!(ch, '"' | '\'' | '`') {
            quote = Some(ch);
            offset += ch.len_utf8();
            continue;
        }
        match ch {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    return Some(offset);
                }
            }
            _ => {}
        }
        offset += ch.len_utf8();
    }
    None
}

#[cfg(test)]
mod tests {
    use super::annotation_offset;

    struct AnnotationSourceCase {
        query: &'static str,
        message: &'static str,
        expected: usize,
    }

    #[test]
    fn resolves_prometheus_annotation_sources() {
        let cases = [
            AnnotationSourceCase {
                query: "max({job=\"api-server\"})",
                message: "PromQL info: ignored histogram in max aggregation",
                expected: 4,
            },
            AnnotationSourceCase {
                query: "count(topk(1000, metric))",
                message: "PromQL info: ignored histogram in topk aggregation",
                expected: 6,
            },
            AnnotationSourceCase {
                query: "quantile without(point)(NaN, data)",
                message: "PromQL warning: quantile value should be between 0 and 1, got NaN",
                expected: 24,
            },
            AnnotationSourceCase {
                query: "histogram_quantile(0.5, series)",
                message: "PromQL warning: vector contains a mix of classic and native histograms",
                expected: 24,
            },
            AnnotationSourceCase {
                query: "histogram_fraction(-Inf, +Inf, histogram_nan)",
                message: "PromQL info: input to histogram_fraction has NaN observations, which are excluded from all fractions",
                expected: 19,
            },
            AnnotationSourceCase {
                query: "rate(metric[1m])",
                message: "PromQL info: metric might not be a counter, __type__ label is not set to \"counter\" or \"histogram\", got \"\": \"metric\"",
                expected: 5,
            },
            AnnotationSourceCase {
                query: "rate(metric_total[20s:5s])",
                message: "PromQL info: metric might not be a counter, __type__ label is not set to \"counter\" or \"histogram\", got \"\": \"metric_total\"",
                expected: 0,
            },
            AnnotationSourceCase {
                query: "sum by (__name__) (rate(metric_total{env=\"2\"}[5m]) or metric_total{env=\"1\"})",
                message: "PromQL info: metric might not be a counter, __type__ label is not set to \"counter\" or \"histogram\", got \"\": \"metric_total\"",
                expected: 24,
            },
            AnnotationSourceCase {
                query: "label_replace(rate({env=\"1\"}[1m]), \"name\", \"rate(fake[1m])\", \"__name__\", \".*\")",
                message: "PromQL info: metric might not be a counter, __type__ label is not set to \"counter\" or \"histogram\", got \"\": \"metric_total\"",
                expected: 19,
            },
            AnnotationSourceCase {
                query: "sum(rate(foo_total[1m]) + increase(bar_total[1m]))",
                message: "PromQL info: metric might not be a counter, __type__ label is not set to \"counter\" or \"histogram\", got \"\": \"foo_total\"",
                expected: 9,
            },
            AnnotationSourceCase {
                query: "sum(rate(sum_over_time(metric_total[30s:10s])[50s:10s]))",
                message: "PromQL info: metric might not be a counter, __type__ label is not set to \"counter\" or \"histogram\", got \"\": \"metric_total\"",
                expected: 0,
            },
            AnnotationSourceCase {
                query: "histogram_count(sum(metric))",
                message: "PromQL warning: conflicting counter resets during histogram aggregation",
                expected: 20,
            },
            AnnotationSourceCase {
                query: "metric{series=\"2\"} + ignoring (series) metric{series=\"3\"}",
                message: "PromQL info: mismatched custom buckets were reconciled during addition",
                expected: 0,
            },
        ];
        for case in cases {
            assert2::assert!(annotation_offset(case.query, case.message) == Some(case.expected));
        }
    }
}
