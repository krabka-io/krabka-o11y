use std::fmt::Write as _;

use promql_parser::parser::Expr;

use super::skip_literal_or_comment;

// An info() data-label selector can match empty strings. The dependency applies
// ordinary vector-selector validation before it sees the enclosing function.
pub(crate) fn normalize_info_selectors(query: &str) -> (String, String) {
    let mut marker = "__krabka_info_data_selector__".to_string();
    while query.contains(&marker) {
        marker.push('_');
    }
    let bytes = query.as_bytes();
    let mut output = String::new();
    let mut start = 0;
    let mut index = 0;
    let mut quote = None;
    let mut braces = 0_u32;
    let mut brackets = 0_u32;
    let mut calls = Vec::<(bool, usize)>::new();
    while index < bytes.len() {
        if skip_literal_or_comment(&mut quote, bytes, &mut index) {
            continue;
        }
        let byte = bytes[index];
        if byte == b'(' {
            let prefix = query[..index].trim_end();
            let name = prefix
                .rsplit(|character: char| !character.is_ascii_alphanumeric() && character != '_')
                .next()
                .unwrap_or("");
            calls.push((name == "info", 0));
        } else if byte == b')' {
            calls.pop();
        } else if byte == b'[' {
            brackets += 1;
        } else if byte == b']' {
            brackets = brackets.saturating_sub(1);
        } else if byte == b'{' {
            if braces == 0 && brackets == 0 && calls.last() == Some(&(true, 1)) {
                output.push_str(&query[start..=index]);
                write!(output, "{marker}=\"true\",").expect("writing into String succeeds");
                start = index + 1;
            }
            braces += 1;
        } else if byte == b'}' {
            braces = braces.saturating_sub(1);
        } else if byte == b','
            && braces == 0
            && brackets == 0
            && let Some((_, arguments)) = calls.last_mut()
        {
            *arguments += 1;
        }
        index += 1;
    }
    output.push_str(&query[start..]);
    (output, marker)
}

pub(crate) fn restore_info_selectors(
    expr: &mut Expr,
    marker: &str,
    starts: &mut std::collections::VecDeque<bool>,
) {
    match expr {
        Expr::Call(call) => {
            if call.func.name == "timestamp" && starts.pop_front().unwrap_or(false) {
                call.func.name = "start_timestamp";
            }
            if call.func.name == "info"
                && let Some(argument) = call.args.args.get_mut(1)
                && let Expr::VectorSelector(selector) = argument.as_mut()
            {
                selector
                    .matchers
                    .matchers
                    .retain(|matcher| matcher.name != marker);
            }
            for argument in &mut call.args.args {
                restore_info_selectors(argument, marker, starts);
            }
        }
        Expr::Aggregate(aggregate) => {
            if let Some(parameter) = &mut aggregate.param {
                restore_info_selectors(parameter, marker, starts);
            }
            restore_info_selectors(&mut aggregate.expr, marker, starts);
        }
        Expr::Binary(binary) => {
            restore_info_selectors(&mut binary.lhs, marker, starts);
            restore_info_selectors(&mut binary.rhs, marker, starts);
        }
        Expr::Paren(paren) => restore_info_selectors(&mut paren.expr, marker, starts),
        Expr::Unary(unary) => restore_info_selectors(&mut unary.expr, marker, starts),
        Expr::Subquery(subquery) => restore_info_selectors(&mut subquery.expr, marker, starts),
        Expr::NumberLiteral(_)
        | Expr::StringLiteral(_)
        | Expr::VectorSelector(_)
        | Expr::MatrixSelector(_)
        | Expr::Extension(_) => {}
    }
}

pub(crate) fn normalize_start_timestamp(query: &str) -> (String, std::collections::VecDeque<bool>) {
    let bytes = query.as_bytes();
    let mut output = String::new();
    let mut starts = std::collections::VecDeque::new();
    let mut copied = 0;
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'"' | b'\'' | b'`' => {
                let delimiter = bytes[index];
                index += 1;
                while index < bytes.len() && bytes[index] != delimiter {
                    if bytes[index] == b'\\' && delimiter != b'`' {
                        index += 1;
                    }
                    index += 1;
                }
                index += 1;
            }
            b'#' => {
                while index < bytes.len() && bytes[index] != b'\n' {
                    index += 1;
                }
            }
            byte if byte.is_ascii_alphabetic() || byte == b'_' => {
                let start = index;
                while index < bytes.len()
                    && (bytes[index].is_ascii_alphanumeric() || bytes[index] == b'_')
                {
                    index += 1;
                }
                let name = &query[start..index];
                if matches!(name, "timestamp" | "start_timestamp")
                    && query[index..].trim_start().starts_with('(')
                {
                    starts.push_back(name == "start_timestamp");
                    if name == "start_timestamp" {
                        output.push_str(&query[copied..start]);
                        output.push_str("timestamp");
                        copied = index;
                    }
                }
            }
            _ => index += 1,
        }
    }
    output.push_str(&query[copied..]);
    (output, starts)
}

#[cfg(test)]
mod tests {
    use promql_parser::parser::{Expr, parse};

    use super::{normalize_info_selectors, restore_info_selectors};

    #[test]
    fn info_data_selectors_accept_empty_matchers_without_changing_vector_validation() {
        for query in [
            r#"info(sum by(job,instance)(up), {data=~".*"})"#,
            "info(up, {})",
        ] {
            let (query, marker) = normalize_info_selectors(query);
            let mut expr = parse(&query).unwrap();
            restore_info_selectors(&mut expr, &marker, &mut std::collections::VecDeque::new());
            let Expr::Call(call) = expr else {
                panic!("call expected")
            };
            let Expr::VectorSelector(selector) = call.args.args[1].as_ref() else {
                panic!("selector expected")
            };
            assert2::assert!(
                selector
                    .matchers
                    .matchers
                    .iter()
                    .all(|matcher| matcher.name != marker)
            );
        }
        let (query, _) = normalize_info_selectors(r#"info({x=~".*"}, {data="info"})"#);
        assert2::assert!(parse(&query).is_err());
    }
}

// start_timestamp has the same vector signature as timestamp; the dependency
// registry predates it. Rewrite only function tokens, then restore their names.
