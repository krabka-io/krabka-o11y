use std::collections::BTreeMap;

use serde_json::Value;

use super::{
    DurationExprContext, DurationExprParser, PromqlError, Result, ToPrimitive, matching_delimiter,
    offset_operand, starts_offset_keyword, top_level_colon,
};
use crate::{format_promql_expr, parse_promql, serialize_promql_expr};

struct Timing {
    seconds: f64,
    expr: Value,
}

impl Timing {
    fn dynamic(&self) -> bool {
        self.expr["type"] != "numberLiteral"
    }
}

// The dependency stores evaluated durations, while the 3.14 AST API exposes
// unevaluated expressions. Bind timing slots to unique durations during this
// API-only parse; the engine never sees these placeholders.
fn rewrite_timings(
    query: &str,
    mut replace: impl FnMut(&str, bool) -> Result<String>,
) -> Result<String> {
    let chars = query.chars().collect::<Vec<_>>();
    let mut output = String::new();
    let mut index = 0;
    while index < chars.len() {
        if chars[index] == '#' {
            while index < chars.len() && chars[index] != '\n' {
                output.push(chars[index]);
                index += 1;
            }
            continue;
        }
        if matches!(chars[index], '"' | '\'' | '`') {
            let quote = chars[index];
            output.push(quote);
            index += 1;
            while index < chars.len() {
                let ch = chars[index];
                output.push(ch);
                index += 1;
                if ch == '\\' && quote != '`' && index < chars.len() {
                    output.push(chars[index]);
                    index += 1;
                } else if ch == quote {
                    break;
                }
            }
            continue;
        }
        if chars[index] == '[' {
            let end = matching_delimiter(&chars, index, '[', ']')?;
            let content = chars[index + 1..end].iter().collect::<String>();
            output.push('[');
            if let Some(colon) = top_level_colon(&content)? {
                output.push_str(&replace(content[..colon].trim(), true)?);
                output.push(':');
                if !content[colon + 1..].trim().is_empty() {
                    output.push_str(&replace(content[colon + 1..].trim(), true)?);
                }
            } else {
                output.push_str(&replace(content.trim(), true)?);
            }
            output.push(']');
            index = end + 1;
            continue;
        }
        if starts_offset_keyword(&chars, index)
            && let Some((operand, end)) = offset_operand(&chars, index + "offset".len())
        {
            output.push_str("offset ");
            output.push_str(&replace(&operand, false)?);
            index = end;
            continue;
        }
        output.push(chars[index]);
        index += 1;
    }
    Ok(output)
}

fn prepare(query: &str) -> Result<(String, BTreeMap<i64, Timing>)> {
    let mut timings = BTreeMap::new();
    let rewritten = rewrite_timings(query, |source, positive| {
        let (seconds, expr) =
            DurationExprParser::new(source, DurationExprContext::instant(0)).parse_with_ast()?;
        let index =
            i64::try_from(timings.len()).map_err(|error| PromqlError::Parse(error.to_string()))?;
        let marker = 1_000_000 + index;
        if expr["type"] == "numberLiteral" {
            if positive && seconds <= 0.0 {
                return Err(PromqlError::Parse(
                    "duration must be greater than 0".to_owned(),
                ));
            }
            super::seconds_to_duration_literal(seconds.abs())?;
        }
        timings.insert(marker, Timing { seconds, expr });
        Ok(format!("{marker}ms"))
    })?;
    Ok((rewritten, timings))
}

/// Serializes the source query using the pinned Prometheus 3.14 AST wire shape,
/// retaining dynamic range, offset, and subquery-step expressions.
///
/// # Errors
/// Returns a parse error for malformed syntax or duration expressions.
pub fn serialize_promql_query(query: &str) -> Result<Value> {
    fn restore(value: &mut Value, timings: &BTreeMap<i64, Timing>) {
        match value {
            Value::Object(fields) => {
                for (field, expr_field) in [
                    ("range", "rangeExpr"),
                    ("offset", "offsetExpr"),
                    ("step", "stepExpr"),
                ] {
                    if let Some(timing) = fields
                        .get(field)
                        .and_then(Value::as_i64)
                        .and_then(|marker| timings.get(&marker))
                    {
                        let milliseconds = if timing.dynamic() {
                            0
                        } else {
                            (timing.seconds * 1000.0)
                                .trunc()
                                .to_i64()
                                .expect("validated duration fits milliseconds")
                        };
                        fields.insert(field.to_owned(), serde_json::json!(milliseconds));
                        fields.insert(
                            expr_field.to_owned(),
                            if timing.dynamic() {
                                timing.expr.clone()
                            } else {
                                Value::Null
                            },
                        );
                    }
                }
                for child in fields.values_mut() {
                    restore(child, timings);
                }
            }
            Value::Array(children) => {
                for child in children {
                    restore(child, timings);
                }
            }
            _ => {}
        }
    }
    let (rewritten, timings) = prepare(query)?;
    let expr = parse_promql(&rewritten)?;
    let mut wire =
        serialize_promql_expr(&expr).map_err(|error| PromqlError::Exec(error.to_string()))?;
    restore(&mut wire, &timings);
    Ok(wire)
}

fn duration_text(node: &Value) -> String {
    if node["type"] == "numberLiteral" {
        let number = node["val"].as_str().unwrap_or_default();
        return if node["duration"] == true {
            let seconds = number
                .parse::<f64>()
                .expect("parser produces numeric literals");
            let sign = if seconds < 0.0 { "-" } else { "" };
            format!(
                "{sign}{}",
                super::seconds_to_duration_literal(seconds.abs()).expect("valid duration literal")
            )
        } else {
            number.to_owned()
        };
    }
    let op = node["op"].as_str().unwrap_or_default();
    let mut text = if matches!(op, "step" | "range" | "start" | "end") {
        format!("{op}()")
    } else if op.chars().all(char::is_alphabetic) || op.contains('_') {
        format!(
            "{op}({}, {})",
            duration_text(&node["lhs"]),
            duration_text(&node["rhs"])
        )
    } else if node["lhs"].is_null() {
        format!("{op}{}", duration_text(&node["rhs"]))
    } else {
        format!(
            "{} {op} {}",
            duration_text(&node["lhs"]),
            duration_text(&node["rhs"])
        )
    };
    if node["wrapped"] == true {
        text = format!("({text})");
    }
    text
}

/// Formats a source query without folding its duration-expression AST.
///
/// # Errors
/// Returns a parse error for malformed syntax or duration expressions.
///
/// # Panics
/// Panics if a previously validated duration cannot be represented by the formatter.
pub fn format_promql_query(query: &str) -> Result<String> {
    let (rewritten, timings) = prepare(query)?;
    let expr = parse_promql(&rewritten)?;
    rewrite_timings(&format_promql_expr(&expr), |source, _| {
        let seconds = DurationExprParser::new(source, DurationExprContext::instant(0)).parse()?;
        let marker = (seconds * 1000.0)
            .round()
            .to_i64()
            .ok_or_else(|| PromqlError::Parse("duration marker out of range".to_owned()))?;
        timings
            .get(&marker)
            .map(|timing| {
                if timing.dynamic() {
                    duration_text(&timing.expr)
                } else {
                    let sign = if timing.seconds < 0.0 { "-" } else { "" };
                    format!(
                        "{sign}{}",
                        super::seconds_to_duration_literal(timing.seconds.abs())
                            .expect("validated duration")
                    )
                }
            })
            .ok_or_else(|| PromqlError::Parse("duration marker missing".to_owned()))
    })
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{format_promql_query, serialize_promql_query};

    #[test]
    fn dynamic_duration_slots_retain_the_pinned_wire_trees_and_static_values() {
        let wire = serialize_promql_query("sum_over_time(up[5m + step()])").unwrap();
        let matrix = &wire["args"][0];
        assert2::assert!(matrix["range"] == 0);
        assert2::assert!(
            matrix["rangeExpr"]
                == json!({"type":"durationExpr","op":"+","lhs":{"type":"numberLiteral","val":"300","duration":true},"rhs":{"type":"durationExpr","op":"step","lhs":null,"rhs":null,"wrapped":false},"wrapped":false})
        );
        let wire = serialize_promql_query("up[10m:step() * 2] offset (range() / 3)").unwrap();
        assert2::assert!(wire["range"] == 600_000);
        assert2::assert!(wire["rangeExpr"].is_null());
        assert2::assert!(wire["step"] == 0);
        assert2::assert!(
            wire["stepExpr"]["rhs"] == json!({"type":"numberLiteral","val":"2","duration":false})
        );
        assert2::assert!(wire["offset"] == 0);
        assert2::assert!(wire["offsetExpr"]["wrapped"] == true);
        assert2::assert!(wire["offsetExpr"]["op"] == "/");
        assert2::assert!(wire["expr"]["offsetExpr"].is_null());
        let wire = serialize_promql_query("up offset -5m").unwrap();
        assert2::assert!(wire["offset"] == -300_000);
        assert2::assert!(wire["offsetExpr"].is_null());
    }

    #[test]
    fn duration_literals_obey_go_nanosecond_bounds_and_zero_divisor_validation() {
        for invalid in [
            "up[1s / 0]",
            "up[1s % 0s]",
            "up[9223372037s]",
            "up[9223372037]",
            "up offset -9223372037s",
        ] {
            assert2::assert!(serialize_promql_query(invalid).is_err(), "{invalid}");
        }
        let positive = serialize_promql_query("up[9223372036s]").unwrap();
        assert2::assert!(positive["range"] == 9_223_372_036_000_i64);
        assert2::assert!(positive["rangeExpr"].is_null());
        // A runtime zero divisor is syntactically valid: API parsing retains
        // the expression; only evaluation can reject the resulting duration.
        let dynamic = serialize_promql_query("up[1s / (step() - step())]").unwrap();
        assert2::assert!(dynamic["range"] == 0);
        assert2::assert!(dynamic["rangeExpr"]["op"] == "/");
        assert2::assert!(dynamic["rangeExpr"]["rhs"]["wrapped"] == true);
    }

    #[test]
    fn duration_formatting_retains_composition_without_replacing_quoted_labels() {
        let query =
            r#"sum_over_time(up{label="16m40s"}[max_of(5m, step() * 2)] offset (range() / 3))"#;
        let formatted = format_promql_query(query).unwrap();
        assert2::assert!(formatted.contains(r#"label="16m40s""#));
        assert2::assert!(formatted.contains("[max_of(5m, step() * 2)]"));
        assert2::assert!(formatted.contains("offset (range() / 3)"));
        assert2::assert!(
            serialize_promql_query(&formatted).unwrap() == serialize_promql_query(query).unwrap()
        );
        for invalid in [
            "up[5m +]",
            "up[step(1)]",
            "up[5m:range(1)]",
            "up offset (1m /)",
        ] {
            assert2::assert!(serialize_promql_query(invalid).is_err(), "{invalid}");
        }
    }
}
