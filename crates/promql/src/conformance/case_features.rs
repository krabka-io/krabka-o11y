//! Parsed witnesses for report queries; these describe syntax, not execution.

use std::convert::Infallible;

use promql_parser::{
    parser::Expr,
    util::{ExprVisitor, walk_expr},
};

/// Type and composition metadata from the parser used by the query engine.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct CaseFeatures {
    /// Parser identity, distinct from the pinned upstream corpus.
    pub parser: &'static str,
    /// Whether the exact original expression parsed successfully.
    pub parsed: bool,
    /// Root expression type, if parsing succeeded.
    pub root_type: Option<String>,
    /// Every syntax node, including aggregate parameters.
    pub nodes: Vec<FeatureNode>,
    /// A syntax/type rejection retains its reason and has no inferred nodes.
    pub parse_error: Option<String>,
}

/// One real AST node; function-looking strings never produce call nodes.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct FeatureNode {
    /// AST variant name.
    pub kind: &'static str,
    /// Return type assigned by the parser.
    pub value_type: String,
    /// Zero-based nesting within the expression.
    pub depth: usize,
    /// Function name, only for a parsed call.
    pub function: Option<String>,
    /// Actual argument types for a parsed call.
    pub argument_types: Vec<String>,
    /// Binary or aggregate operator, only for those node variants.
    pub operator: Option<String>,
    /// Exact parsed scalar literal, including bounds lowered by the engine.
    pub literal_value: Option<String>,
}

// Experimental:true in pinned Prometheus 3.14 parser/functions.go. Keep the
// report classifier source-bound; tools/query-language-inventory.py verifies
// this exact set against the pinned extracted registry.
pub(crate) const EXPERIMENTAL_FUNCTIONS: &[&str] = &[
    "double_exponential_smoothing",
    "end",
    "histogram_quantiles",
    "info",
    "mad_over_time",
    "max_of",
    "min_of",
    "range",
    "sort_by_label",
    "sort_by_label_desc",
    "start",
    "start_timestamp",
    "step",
    "ts_of_first_over_time",
    "ts_of_last_over_time",
    "ts_of_max_over_time",
    "ts_of_min_over_time",
];

impl CaseFeatures {
    pub(crate) fn disabled_features(
        &self,
        query: &str,
        evaluation_error: Option<&str>,
    ) -> Vec<String> {
        if cfg!(feature = "experimental-functions") {
            return Vec::new();
        }
        let Some(error) = evaluation_error else {
            return Vec::new();
        };
        let mut disabled = Vec::new();
        for name in EXPERIMENTAL_FUNCTIONS {
            let parsed = self
                .nodes
                .iter()
                .any(|node| node.function.as_deref() == Some(name));
            let gate = error.contains(&format!(
                "function `{name}` requires the experimental-functions feature"
            )) || error.contains(&format!("function {name} is not enabled"))
                || error.contains(&format!("function \"{name}\" is not enabled"));
            let scalar_boundary = matches!(*name, "start" | "end" | "range" | "step")
                && (error.contains("planner did not claim range expression:")
                    || error.contains("invalid promql query"))
                && unquoted_call(query, name)
                && boundary_helpers_are_well_typed(query, &["start", "end", "range", "step"]);
            if (gate && (parsed || unquoted_call(query, name))) || scalar_boundary {
                disabled.push((*name).to_owned());
            }
        }
        // Pinned parser/lex.go IsExperimentalAggregator identifies these two.
        // The range planner's disabled branch declines the expression rather
        // than executing a limit; retain that observed planner error verbatim.
        for name in ["limitk", "limit_ratio"] {
            let parsed = self
                .nodes
                .iter()
                .any(|node| node.kind == "aggregate" && node.operator.as_deref() == Some(name));
            let gate = error.contains(&format!(
                "aggregation `{name}` requires the experimental-functions feature"
            )) || error.contains("planner did not claim range expression:");
            if parsed && gate {
                disabled.push(name.to_owned());
            }
        }
        disabled
    }

    /// Parse an exact report query without making any coverage claim.
    #[must_use]
    pub fn from_query(query: &str) -> Self {
        Self::from_query_with_context(query, crate::DurationExprContext::instant(0))
    }

    /// Use the real evaluation context for the engine's duration normalization.
    #[must_use]
    pub fn from_query_with_context(query: &str, context: crate::DurationExprContext) -> Self {
        let mut metadata = Self {
            parser: "krabka-promql-front-end/promql-parser-0.10.0",
            parsed: false,
            root_type: None,
            nodes: Vec::new(),
            parse_error: None,
        };
        let expression = match crate::parse_promql_with_duration_context(query, context) {
            Ok(expression) => expression,
            Err(error) => {
                metadata.parse_error = Some(error.to_string());
                return metadata;
            }
        };
        metadata.parsed = true;
        metadata.root_type = Some(expression.value_type().to_string());
        let mut visitor = FeatureVisitor {
            nodes: &mut metadata.nodes,
            depth: 0,
        };
        walk_expr(&mut visitor, &expression).expect("infallible metadata visitor");
        metadata
    }
}

pub(crate) fn disabled_limit_aggregators(features: &CaseFeatures) -> Vec<String> {
    ["limitk", "limit_ratio"]
        .into_iter()
        .filter(|name| {
            features
                .nodes
                .iter()
                .any(|node| node.kind == "aggregate" && node.operator.as_deref() == Some(*name))
        })
        .map(str::to_owned)
        .collect()
}

/// A rejected parser call needs token binding, never substring/quoted matching.
fn unquoted_calls(query: &str, expected: &str) -> Vec<(usize, usize)> {
    let bytes = query.as_bytes();
    let mut index = 0;
    let mut calls = Vec::new();
    while index < bytes.len() {
        if matches!(bytes[index], b'"' | b'\'' | b'`') {
            let delimiter = bytes[index];
            index += 1;
            while index < bytes.len() {
                let byte = bytes[index];
                index += 1;
                if byte == b'\\' && delimiter != b'`' {
                    index = (index + 1).min(bytes.len());
                } else if byte == delimiter {
                    break;
                }
            }
        } else if bytes[index] == b'#' {
            while index < bytes.len() && bytes[index] != b'\n' {
                index += 1;
            }
        } else if bytes[index].is_ascii_alphabetic() || bytes[index] == b'_' {
            let start = index;
            while index < bytes.len()
                && (bytes[index].is_ascii_alphanumeric() || matches!(bytes[index], b'_' | b':'))
            {
                index += 1;
            }
            if &query[start..index] == expected {
                while index < bytes.len() && bytes[index].is_ascii_whitespace() {
                    index += 1;
                }
                if bytes.get(index) == Some(&b'(') {
                    calls.push((start, index));
                }
            }
        } else {
            index += 1;
        }
    }
    calls
}

fn unquoted_call(query: &str, expected: &str) -> bool {
    !unquoted_calls(query, expected).is_empty()
}

// The dependency cannot parse new scalar boundary calls in a disabled build.
// Replace only exact, zero-argument call tokens for a type check, retaining all
// other syntax. A quoted name, @ modifier, invalid argument or unknown outer
// function cannot qualify as a disabled scalar function through this fallback.
fn boundary_helpers_are_well_typed(query: &str, names: &[&str]) -> bool {
    let mut replacements = Vec::new();
    for name in names {
        for (start, open) in unquoted_calls(query, name) {
            if query[..start]
                .bytes()
                .rev()
                .find(|byte| !byte.is_ascii_whitespace())
                == Some(b'@')
            {
                continue;
            }
            let mut close = open + 1;
            while query
                .as_bytes()
                .get(close)
                .is_some_and(u8::is_ascii_whitespace)
            {
                close += 1;
            }
            if query.as_bytes().get(close) != Some(&b')') {
                return false;
            }
            replacements.push((start, close + 1));
        }
    }
    if replacements.is_empty() {
        return false;
    }
    replacements.sort_unstable();
    let mut normalized = query.to_owned();
    for (start, end) in replacements.into_iter().rev() {
        normalized.replace_range(start..end, "(0)");
    }
    promql_parser::parser::parse(&normalized).is_ok()
}

struct FeatureVisitor<'a> {
    nodes: &'a mut Vec<FeatureNode>,
    depth: usize,
}

impl ExprVisitor for FeatureVisitor<'_> {
    type Error = Infallible;

    fn pre_visit(&mut self, expression: &Expr) -> Result<bool, Self::Error> {
        let kind = match expression {
            Expr::Aggregate(_) => "aggregate",
            Expr::Unary(_) => "unary",
            Expr::Binary(_) => "binary",
            Expr::Paren(_) => "paren",
            Expr::Subquery(_) => "subquery",
            Expr::NumberLiteral(_) => "number",
            Expr::StringLiteral(_) => "string",
            Expr::VectorSelector(_) => "vector_selector",
            Expr::MatrixSelector(_) => "matrix_selector",
            Expr::Call(_) => "call",
            Expr::Extension(_) => "extension",
        };
        let (function, argument_types) = match expression {
            Expr::Call(call) => (
                Some(call.func.name.to_owned()),
                call.args
                    .args
                    .iter()
                    .map(|arg| arg.value_type().to_string())
                    .collect(),
            ),
            _ => (None, Vec::new()),
        };
        let operator = match expression {
            Expr::Aggregate(aggregate) => Some(aggregate.op.to_string()),
            Expr::Binary(binary) => Some(match binary.op.id() {
                crate::planner::histogram_trim_operators::HISTOGRAM_TRIM_UPPER => "</".to_owned(),
                crate::planner::histogram_trim_operators::HISTOGRAM_TRIM_LOWER => ">/".to_owned(),
                _ => binary.op.to_string(),
            }),
            _ => None,
        };
        self.nodes.push(FeatureNode {
            kind,
            value_type: expression.value_type().to_string(),
            depth: self.depth,
            function,
            argument_types,
            operator,
            literal_value: match expression {
                Expr::NumberLiteral(number) => Some(number.val.to_string()),
                _ => None,
            },
        });
        self.depth += 1;
        // The dependency's walker visits the aggregate input but omits its
        // scalar parameter; that parameter can itself contain nested calls.
        if let Expr::Aggregate(aggregate) = expression
            && let Some(parameter) = &aggregate.param
        {
            walk_expr(self, parameter)?;
        }
        Ok(true)
    }

    fn post_visit(&mut self, _: &Expr) -> Result<bool, Self::Error> {
        self.depth -= 1;
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn disabled_limit_metadata_uses_aggregate_nodes_with_grouping() {
        for query in [
            "count(limitk by (group) (2, metric))",
            "sum(limitk without (instance) (2, metric))",
        ] {
            let features = CaseFeatures::from_query(query);
            assert!(features.parsed);
            assert!(disabled_limit_aggregators(&features) == vec!["limitk"]);
        }
        let features = CaseFeatures::from_query("limit_ratio(0.5, metric)");
        assert!(disabled_limit_aggregators(&features) == vec!["limit_ratio"]);
        let features = CaseFeatures::from_query("metric{label=\"limitk by(group) (2, metric)\"}");
        assert!(disabled_limit_aggregators(&features).is_empty());
    }

    #[test]
    fn real_nested_calls_record_types_and_aggregate_parameters() {
        let metadata = CaseFeatures::from_query("topk(scalar(sum(metric)), abs(rate(metric[5m])))");
        assert!(metadata.parsed);
        assert!(metadata.root_type.as_deref() == Some("vector"));
        let calls = metadata
            .nodes
            .iter()
            .filter_map(|node| node.function.as_deref())
            .collect::<Vec<_>>();
        assert!(calls == vec!["scalar", "abs", "rate"]);
        let rate = metadata
            .nodes
            .iter()
            .find(|node| node.function.as_deref() == Some("rate"))
            .unwrap();
        assert!(rate.argument_types == vec!["matrix"]);
        assert!(rate.value_type == "vector");
        assert!(rate.depth == 2);
        assert!(
            metadata
                .nodes
                .iter()
                .filter(|node| node.kind == "aggregate")
                .count()
                == 2
        );
    }

    #[test]
    fn restored_histogram_trim_operators_have_canonical_metadata() {
        let metadata = CaseFeatures::from_query("histogram_metric </ 1 >/ 0");
        assert!(metadata.parsed);
        let operators = metadata
            .nodes
            .iter()
            .filter_map(|node| node.operator.as_deref())
            .collect::<Vec<_>>();
        assert!(operators == vec![">/", "</"]);
    }

    #[cfg(feature = "experimental-functions")]
    #[test]
    fn duration_bound_helpers_record_actual_lowered_literals_without_invented_calls() {
        for (query, expected) in [("start()", "10"), ("end()", "30")] {
            let metadata = CaseFeatures::from_query_with_context(
                query,
                crate::DurationExprContext::range(10_000, 30_000, krabka_units::prelude::secs(10)),
            );
            assert!(metadata.parsed);
            assert!(metadata.nodes.iter().all(|node| node.function.is_none()));
            assert!(
                metadata
                    .nodes
                    .iter()
                    .any(|node| node.literal_value.as_deref() == Some(expected))
            );
        }
    }

    #[test]
    fn feature_gate_classification_requires_source_bound_call_and_actual_gate_error() {
        let metadata =
            CaseFeatures::from_query("abs(double_exponential_smoothing(metric[5m],0.1,0.2))");
        let gate = "unsupported: function `double_exponential_smoothing` requires the experimental-functions feature";
        assert!(
            metadata.disabled_features(
                "abs(double_exponential_smoothing(metric[5m],0.1,0.2))",
                Some(gate)
            ) == if cfg!(feature = "experimental-functions") {
                vec![]
            } else {
                vec!["double_exponential_smoothing".to_owned()]
            }
        );
        assert!(
            metadata
                .disabled_features("abs(metric)", Some("internal storage failure"))
                .is_empty()
        );
        assert!(!unquoted_call(
            r#"metric{description="start_timestamp(metric)"}"#,
            "start_timestamp"
        ));
        assert!(!unquoted_call(
            "metric # start_timestamp(metric)",
            "start_timestamp"
        ));
        assert!(!unquoted_call(
            "not_start_timestamp(metric)",
            "start_timestamp"
        ));
        assert!(unquoted_call(
            "sum(start_timestamp(metric))",
            "start_timestamp"
        ));
        let limit = CaseFeatures::from_query("count(limitk(1,metric))");
        let disabled = limit.disabled_features(
            "count(limitk(1,metric))",
            Some("plan error: planner did not claim range expression: count(limitk(1,metric))"),
        );
        assert!(
            disabled
                == if cfg!(feature = "experimental-functions") {
                    vec![]
                } else {
                    vec!["limitk".to_owned()]
                }
        );
    }

    #[test]
    fn quoted_function_names_and_rejections_never_infer_call_coverage() {
        let quoted = CaseFeatures::from_query(r#"metric{description="abs(rate(fake[5m]))"}"#);
        assert!(quoted.parsed);
        assert!(quoted.nodes.iter().all(|node| node.function.is_none()));
        let invalid = CaseFeatures::from_query("abs(metric, 0)");
        assert!(!invalid.parsed);
        assert!(invalid.nodes.is_empty());
        assert!(invalid.parse_error.is_some());
    }
}
