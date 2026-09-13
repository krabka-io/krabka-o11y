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
                    .rsplit_once('\n')
                    .map_or(before.len(), |(_, line)| line.len())
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
        if rate_argument_is_subquery(query) {
            return ["rate", "increase"]
                .into_iter()
                .find_map(|name| find_word(query, name, 0));
        }
        return quoted_tail(message)
            .and_then(|metric| rfind_identifier(query, metric))
            .or_else(|| last_rate_argument(query));
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
        return ["sum_over_time", "avg_over_time"]
            .into_iter()
            .find_map(|name| call_argument(query, name, 0))
            .or_else(|| aggregate_argument(query, "sum", 0))
            .or_else(|| aggregate_argument(query, "avg", 0));
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
        return ["sum_over_time", "avg_over_time"]
            .into_iter()
            .find_map(|name| call_argument(query, name, 0))
            .or_else(|| aggregate_argument(query, "sum", 0))
            .or_else(|| aggregate_argument(query, "avg", 0));
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

fn quoted_tail(message: &str) -> Option<&str> {
    let end = message.strip_suffix('"')?;
    let (_, value) = end.rsplit_once('"')?;
    Some(value)
}

fn first_expression_byte(query: &str) -> Option<usize> {
    query
        .char_indices()
        .find(|(_, ch)| !ch.is_whitespace())
        .map(|(offset, _)| offset)
}

fn rfind_identifier(query: &str, identifier: &str) -> Option<usize> {
    query
        .match_indices(identifier)
        .filter(|(offset, _)| {
            let before = query[..*offset].chars().next_back();
            let after = query[*offset + identifier.len()..].chars().next();
            !before.is_some_and(is_identifier_char) && !after.is_some_and(is_identifier_char)
        })
        .map(|(offset, _)| offset)
        .last()
}

fn is_identifier_char(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || matches!(ch, '_' | ':' | '.')
}

fn last_rate_argument(query: &str) -> Option<usize> {
    ["rate", "increase"]
        .into_iter()
        .filter_map(|name| call_argument(query, name, 0))
        .max()
}

fn rate_argument_is_subquery(query: &str) -> bool {
    ["rate", "increase"].into_iter().any(|name| {
        let Some(start) = find_word(query, name, 0) else {
            return false;
        };
        let Some(open) = query[start + name.len()..]
            .find('(')
            .map(|offset| start + name.len() + offset)
        else {
            return false;
        };
        let Some(close) = matching_paren(query, open) else {
            return false;
        };
        let argument = &query[open + 1..close];
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
    for (relative, ch) in query[open + 1..].char_indices() {
        let offset = open + 1 + relative;
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
    query[from..]
        .match_indices(word)
        .find(|(relative, _)| {
            let offset = from + relative;
            let before = query[..offset].chars().next_back();
            let after = query[offset + word.len()..].chars().next();
            !before.is_some_and(is_identifier_char) && !after.is_some_and(is_identifier_char)
        })
        .map(|(relative, _)| from + relative)
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
    for (relative, ch) in query[open..].char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    return Some(open + relative);
                }
            }
            _ => {}
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::annotation_offset;

    #[test]
    fn resolves_prometheus_annotation_sources() {
        let cases = [
            (
                "max({job=\"api-server\"})",
                "PromQL info: ignored histogram in max aggregation",
                4,
            ),
            (
                "count(topk(1000, metric))",
                "PromQL info: ignored histogram in topk aggregation",
                6,
            ),
            (
                "quantile without(point)(NaN, data)",
                "PromQL warning: quantile value should be between 0 and 1, got NaN",
                24,
            ),
            (
                "histogram_quantile(0.5, series)",
                "PromQL warning: vector contains a mix of classic and native histograms",
                24,
            ),
            (
                "histogram_fraction(-Inf, +Inf, histogram_nan)",
                "PromQL info: input to histogram_fraction has NaN observations, which are excluded from all fractions",
                19,
            ),
            (
                "rate(metric[1m])",
                "PromQL info: metric might not be a counter, __type__ label is not set to \"counter\" or \"histogram\", got \"\": \"metric\"",
                5,
            ),
            (
                "rate(metric_total[20s:5s])",
                "PromQL info: metric might not be a counter, __type__ label is not set to \"counter\" or \"histogram\", got \"\": \"metric_total\"",
                0,
            ),
            (
                "histogram_count(sum(metric))",
                "PromQL warning: conflicting counter resets during histogram aggregation",
                20,
            ),
            (
                "metric{series=\"2\"} + ignoring (series) metric{series=\"3\"}",
                "PromQL info: mismatched custom buckets were reconciled during addition",
                0,
            ),
        ];
        for (query, message, expected) in cases {
            assert_eq!(annotation_offset(query, message), Some(expected), "{query}");
        }
    }
}
