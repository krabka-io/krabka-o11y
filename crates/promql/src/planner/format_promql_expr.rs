use promql_parser::{parser::Expr, util::display_duration};

use super::{
    ExtendedSelectorExpr,
    histogram_trim_operators::{HISTOGRAM_TRIM_LOWER, HISTOGRAM_TRIM_UPPER},
};

/// Formats a parsed expression, including operators added after the parser dependency's release.
#[must_use]
pub fn format_promql_expr(expr: &Expr) -> String {
    match expr {
        Expr::Binary(binary) => {
            let operator = match binary.op.id() {
                HISTOGRAM_TRIM_UPPER => "</".to_owned(),
                HISTOGRAM_TRIM_LOWER => ">/".to_owned(),
                _ => binary.op.to_string(),
            };
            let modifier = binary
                .modifier
                .as_ref()
                .map_or_else(String::new, ToString::to_string);
            format!(
                "{} {operator}{modifier} {}",
                format_promql_expr(&binary.lhs),
                format_promql_expr(&binary.rhs)
            )
        }
        Expr::Aggregate(aggregate) => {
            let grouping = match &aggregate.modifier {
                Some(promql_parser::parser::LabelModifier::Exclude(labels)) => {
                    format!(" without ({labels}) ")
                }
                Some(promql_parser::parser::LabelModifier::Include(labels))
                    if !labels.is_empty() =>
                {
                    format!(" by ({labels}) ")
                }
                _ => String::new(),
            };
            let parameter = aggregate.param.as_ref().map_or_else(String::new, |param| {
                format!("{}, ", format_promql_expr(param))
            });
            format!(
                "{}{grouping}({parameter}{})",
                aggregate.op,
                format_promql_expr(&aggregate.expr)
            )
        }
        Expr::Paren(paren) => format!("({})", format_promql_expr(&paren.expr)),
        Expr::Unary(unary) => format!("-{}", format_promql_expr(&unary.expr)),
        Expr::Call(call) => format!(
            "{}({})",
            call.func.name,
            call.args
                .args
                .iter()
                .map(|arg| format_promql_expr(arg))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        Expr::Subquery(subquery) => {
            let step = subquery
                .step
                .as_ref()
                .map_or_else(String::new, display_duration);
            let at = subquery
                .at
                .as_ref()
                .map_or_else(String::new, |at| format!(" {at}"));
            let offset = subquery
                .offset
                .as_ref()
                .map_or_else(String::new, |offset| format!(" offset {offset}"));
            format!(
                "{}[{}:{step}]{at}{offset}",
                format_promql_expr(&subquery.expr),
                display_duration(&subquery.range)
            )
        }
        Expr::Extension(extension) => {
            if let Some(selector) = extension
                .expr
                .as_any()
                .downcast_ref::<super::byte_selector_expr::ByteSelectorExpr>()
            {
                return selector.format();
            }
            if let Some(value) = super::byte_string_expr::string_expr_value(expr) {
                return krabka_logql::quote_go_bytes(value.as_bytes());
            }
            if let Some(selector) = extension
                .expr
                .as_any()
                .downcast_ref::<ExtendedSelectorExpr>()
                && let Some(child) = selector.child()
            {
                format!(
                    "{} {}",
                    format_promql_expr(child),
                    selector.modifier().keyword()
                )
            } else {
                expr.to_string()
            }
        }
        Expr::NumberLiteral(_)
        | Expr::StringLiteral(_)
        | Expr::VectorSelector(_)
        | Expr::MatrixSelector(_) => expr.to_string(),
    }
}

/// Serializes the AST without exposing private histogram operator token identifiers.
///
/// The dependency defines the standard JSON shape. Only operator fields corresponding
/// to actual binary nodes are changed; user strings and matcher values are preserved.
///
/// # Errors
/// Returns an error if the parser dependency cannot serialize an AST node.
///
/// # Panics
/// Panics if the parser dependency serializes function metadata as a non-object.
pub fn serialize_promql_expr(expr: &Expr) -> Result<serde_json::Value, serde_json::Error> {
    // Serialize each parent with harmless placeholders so nested extension nodes
    // do not reach the dependency's deliberately skipped Extension serializer.
    let placeholder = || {
        Box::new(Expr::NumberLiteral(
            promql_parser::parser::NumberLiteral::new(0.0),
        ))
    };
    let mut shallow = expr.clone();
    match &mut shallow {
        Expr::Binary(binary) => {
            binary.lhs = placeholder();
            binary.rhs = placeholder();
        }
        Expr::Aggregate(aggregate) => {
            aggregate.expr = placeholder();
            if aggregate.param.is_some() {
                aggregate.param = Some(placeholder());
            }
        }
        Expr::Call(call) => call.args.args.clear(),
        Expr::Paren(paren) => paren.expr = placeholder(),
        Expr::Unary(unary) => unary.expr = placeholder(),
        Expr::Subquery(subquery) => subquery.expr = placeholder(),
        Expr::Extension(extension) => {
            if let Some(selector) = extension
                .expr
                .as_any()
                .downcast_ref::<super::byte_selector_expr::ByteSelectorExpr>()
            {
                return serialize_promql_expr(&selector.child);
            }
            if let Some(value) = super::byte_string_expr::string_expr_value(expr) {
                return Ok(serde_json::json!({"type":"stringLiteral","val":value.as_str()}));
            }
            if let Some(selector) = extension
                .expr
                .as_any()
                .downcast_ref::<ExtendedSelectorExpr>()
                && let Some(child) = selector.child()
            {
                let mut value = serialize_promql_expr(child)?;
                value[selector.modifier().keyword()] = true.into();
                return Ok(value);
            }
        }
        _ => {}
    }
    let mut value = serde_json::to_value(shallow)?;
    match expr {
        Expr::Binary(binary) => {
            match binary.op.id() {
                HISTOGRAM_TRIM_UPPER => value["op"] = "</".into(),
                HISTOGRAM_TRIM_LOWER => value["op"] = ">/".into(),
                _ => {}
            }
            value["matching"] = if binary.lhs.value_type()
                == promql_parser::parser::value::ValueType::Vector
                && binary.rhs.value_type() == promql_parser::parser::value::ValueType::Vector
            {
                use promql_parser::parser::{LabelModifier, VectorMatchCardinality};
                let modifier = binary.modifier.as_ref();
                let card = match modifier.map(|modifier| &modifier.card) {
                    Some(VectorMatchCardinality::ManyToOne(_)) => "many-to-one",
                    Some(VectorMatchCardinality::OneToMany(_)) => "one-to-many",
                    Some(VectorMatchCardinality::ManyToMany) => "many-to-many",
                    _ => "one-to-one",
                };
                serde_json::json!({
                    "card": card,
                    "labels": modifier.and_then(|modifier| modifier.matching.as_ref()).map(LabelModifier::labels).map_or_else(|| serde_json::json!([]), |labels| serde_json::json!(labels)),
                    "on": matches!(modifier.and_then(|modifier| modifier.matching.as_ref()), Some(LabelModifier::Include(_))),
                    "include": modifier.and_then(|modifier| modifier.card.labels()).map_or_else(|| serde_json::json!([]), |labels| serde_json::json!(labels)),
                    "fillValues": { "lhs": modifier.and_then(|modifier| modifier.fill_values.lhs), "rhs": modifier.and_then(|modifier| modifier.fill_values.rhs) }
                })
            } else {
                serde_json::Value::Null
            };
            value["lhs"] = serialize_promql_expr(&binary.lhs)?;
            value["rhs"] = serialize_promql_expr(&binary.rhs)?;
        }
        Expr::Aggregate(aggregate) => {
            if let Some(parameter) = &aggregate.param {
                value["param"] = serialize_promql_expr(parameter)?;
            }
            value["expr"] = serialize_promql_expr(&aggregate.expr)?;
        }
        Expr::Call(call) => {
            value["func"]
                .as_object_mut()
                .expect("serialized function is an object")
                .remove("experimental");
            value["args"] = call
                .args
                .args
                .iter()
                .map(|argument| serialize_promql_expr(argument))
                .collect::<Result<Vec<_>, _>>()?
                .into();
        }
        Expr::Paren(paren) => value["expr"] = serialize_promql_expr(&paren.expr)?,
        Expr::Unary(unary) => value["expr"] = serialize_promql_expr(&unary.expr)?,
        Expr::Subquery(subquery) => {
            value["expr"] = serialize_promql_expr(&subquery.expr)?;
            for field in ["rangeExpr", "offsetExpr", "stepExpr"] {
                value[field] = serde_json::Value::Null;
            }
            if subquery.step.is_none() {
                value["step"] = 0.into();
            }
        }
        Expr::VectorSelector(_) | Expr::MatrixSelector(_) => {
            value["offsetExpr"] = serde_json::Value::Null;
            value["anchored"] = false.into();
            value["smoothed"] = false.into();
            if matches!(expr, Expr::MatrixSelector(_)) {
                value["rangeExpr"] = serde_json::Value::Null;
            }
            if value["name"].is_null() {
                value["name"] = "".into();
            }
        }
        _ => {}
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn byte_strings_use_go_quotes_and_preserve_selector_name_and_timing() {
        for query in [
            r#"label_join(m, "copy", "☃\xff", "raw")"#,
            r#"m{raw="☃\xff"}[5m] offset 1m"#,
        ] {
            let expr = super::super::parse_promql(query).unwrap();
            let formatted = format_promql_expr(&expr);
            assert2::assert!(formatted == query);
            assert2::assert!(super::super::parse_promql(&formatted).unwrap() == expr);
        }
        let expression = super::super::parse_promql(r#"m{raw="☃\xff"}"#).unwrap();
        assert2::assert!(
            serialize_promql_expr(&expression).unwrap()["matchers"][0]["value"] == "☃�"
        );
    }

    #[test]
    fn composed_trim_formatting_round_trips_without_changing_strings_or_comparisons() {
        let query =
            r#"sum by (job) (histogram_count((h{label="unknown token >/"} </ 2) >/ 0)) > 1"#;
        let expr = super::super::parse_promql(query).unwrap();
        let formatted = format_promql_expr(&expr);
        assert2::assert!(formatted == query);
        assert2::assert!(super::super::parse_promql(&formatted).unwrap() == expr);
        let json = serialize_promql_expr(&expr).unwrap();
        assert2::assert!(json["op"] == ">");
        assert2::assert!(json["lhs"]["expr"]["args"][0]["op"] == ">/");
        assert2::assert!(json["lhs"]["expr"]["args"][0]["lhs"]["expr"]["op"] == "</");
        assert2::assert!(
            json["lhs"]["expr"]["args"][0]["lhs"]["expr"]["lhs"]["matchers"]
                == serde_json::json!([{ "name": "label", "type": "=", "value": "unknown token >/" }])
        );
    }

    #[test]
    fn trimmed_subqueries_preserve_timing_and_extended_selector_modifiers() {
        for query in [
            "(h </ 2)[10m:30s] @ start() offset -1m",
            "rate(h[5m] anchored) </ 2",
        ] {
            let expr = super::super::parse_promql(query).unwrap();
            let formatted = format_promql_expr(&expr);
            assert2::assert!(super::super::parse_promql(&formatted).unwrap() == expr);
            let json = serialize_promql_expr(&expr).unwrap();
            if query.starts_with("rate") {
                assert2::assert!(json["lhs"]["args"][0]["anchored"] == true);
            }
        }
    }

    #[test]
    fn versioned_ast_json_preserves_matching_labels_fill_and_selector_fields() {
        let expr =
            super::super::parse_promql("a + on (job) group_left (zone) fill_left (0) b").unwrap();
        let value = serialize_promql_expr(&expr).unwrap();
        let selector = |name| {
            serde_json::json!({
                "type":"vectorSelector", "name":name, "matchers":[], "offset":0,
                "offsetExpr":null, "timestamp":null, "startOrEnd":null,
                "anchored":false, "smoothed":false
            })
        };
        assert2::assert!(
            value
                == serde_json::json!({
                    "type":"binaryExpr", "op":"+", "lhs":selector("a"), "rhs":selector("b"),
                    "matching":{"card":"many-to-one","include":["zone"],"labels":["job"],"on":true,"fillValues":{"lhs":0.0,"rhs":null}}, "bool":false
                })
        );
        let scalar = serialize_promql_expr(&super::super::parse_promql("1 + 2").unwrap()).unwrap();
        assert2::assert!(scalar["matching"].is_null());
        let matrix =
            serialize_promql_expr(&super::super::parse_promql("rate(a[5m] anchored)").unwrap())
                .unwrap();
        assert2::assert!(
            matrix
                == serde_json::json!({
                    "type":"call", "func":{"name":"rate","argTypes":["matrix"],"variadic":0,"returnType":"vector"},
                    "args":[{"type":"matrixSelector","name":"a","matchers":[],"range":300000,"rangeExpr":null,"offset":0,"offsetExpr":null,"timestamp":null,"startOrEnd":null,"anchored":true,"smoothed":false}]
                })
        );
        let subquery =
            serialize_promql_expr(&super::super::parse_promql("(a)[5m:] offset -1m").unwrap())
                .unwrap();
        assert2::assert!(
            subquery["range"] == 300000 && subquery["step"] == 0 && subquery["offset"] == -60000
        );
        for key in ["rangeExpr", "stepExpr", "offsetExpr"] {
            assert2::assert!(subquery[key].is_null());
        }
    }
}
