use std::fmt;

use crate::{
    ComparisonOp, DestinationLabel, DurationNanos, FieldFilter, FieldFilterExpression,
    FieldFilterLogicOp, FieldValue, IpMatcher, JsonExpressionPath, JsonExtraction,
    JsonParserConfig, LabelFormat, LabelFormatAssignment, LabelMatcher, LabelSelection,
    LabelSelectionSet, LineFilter, LineFilterOp, LineFormat, LogfmtExtraction, LogfmtParserConfig,
    MatchOp, OffsetNanos, ParseError, ParserStage, PatternParser, PipelineStage,
    QuantileDenominator, QuantileNumerator, RegexpParser, SourceLabel, StreamQuery,
    UnwrapExpression,
    filters::field_filter_expression_to_pipeline_stage,
    util::{
        QuotedBodyError, QuotedChar, gcd_u64, is_ident_char, is_ident_start, parse_bytes_literal,
        parse_prometheus_duration_literal, prometheus_duration_unit, read_quoted_body,
    },
};
mod format_matching;
mod logql_expr;
mod outer_metric_parentheses_inner;
mod parse_expr_primary;
mod parse_variants;
mod parser;
use format_matching::format_matching;
pub use logql_expr::LogqlExpr;
use outer_metric_parentheses_inner::{outer_metric_parentheses_inner, split_metric_parentheses};
use parse_expr_primary::parse_expr_primary;
use parse_variants::parse_variants;
use parser::Parser;

use crate::error::syntax_parse_error as syntax_error;

pub(crate) fn format_labels(labels: &[String]) -> String {
    if labels.is_empty() {
        String::new()
    } else {
        format!("({})", labels.join(", "))
    }
}

/// # Errors
/// Returns an error when the query or template is malformed, a requested conversion is invalid, or evaluation cannot read its input data.
pub fn parse_metric_binary_arithmetic_query(
    input: &str,
) -> Result<MetricBinaryArithmetic, ParseError> {
    Parser::new(input).parse_metric_binary_arithmetic()
}

/// # Errors
/// Returns an error when the query or template is malformed, a requested conversion is invalid, or evaluation cannot read its input data.
pub fn parse_metric_binary_comparison_query(
    input: &str,
) -> Result<MetricBinaryComparison, ParseError> {
    Parser::new(input).parse_metric_binary_comparison()
}

/// # Errors
/// Returns an error when the query or template is malformed, a requested conversion is invalid, or evaluation cannot read its input data.
pub fn parse_metric_binary_set_query(input: &str) -> Result<MetricBinarySet, ParseError> {
    Parser::new(input).parse_metric_binary_set()
}

/// # Errors
/// Returns an error when the query or template is malformed, a requested conversion is invalid, or evaluation cannot read its input data.
pub fn parse_metric_label_join_query(input: &str) -> Result<MetricLabelJoin, ParseError> {
    Parser::new(input).parse_metric_label_join()
}

/// # Errors
/// Returns an error when the query or template is malformed, a requested conversion is invalid, or evaluation cannot read its input data.
pub fn parse_metric_label_replace_query(input: &str) -> Result<MetricLabelReplace, ParseError> {
    Parser::new(input).parse_metric_label_replace()
}

#[tracing::instrument(level = "info", skip_all, fields(query = %input), err)]
/// # Errors
/// Returns an error when the query or template is malformed, a requested conversion is invalid, or evaluation cannot read its input data.
pub fn parse_metric_query(input: &str) -> Result<MetricQuery, ParseError> {
    Parser::new(input).parse_metric()
}

/// # Errors
/// Returns an error when the query or template is malformed, a requested conversion is invalid, or evaluation cannot read its input data.
pub fn parse_metric_scalar_arithmetic_query(
    input: &str,
) -> Result<MetricScalarArithmetic, ParseError> {
    Parser::new(input).parse_metric_scalar_arithmetic()
}

/// # Errors
/// Returns an error when the query or template is malformed, a requested conversion is invalid, or evaluation cannot read its input data.
pub fn parse_metric_scalar_comparison_query(
    input: &str,
) -> Result<MetricScalarComparison, ParseError> {
    Parser::new(input).parse_metric_scalar_comparison()
}

pub(crate) fn parse_metric_subexpression(input: &str) -> Result<MetricQuery, ParseError> {
    Parser::new(strip_outer_metric_parentheses(input)).parse_metric()
}

#[tracing::instrument(level = "info", skip_all, fields(query = %input), err)]
/// # Errors
/// Returns an error when the query or template is malformed, a requested conversion is invalid, or evaluation cannot read its input data.
pub fn parse_query(input: &str) -> Result<StreamQuery, ParseError> {
    Parser::new(input).parse()
}

pub(crate) fn parse_scalar_text(input: &str) -> bool {
    let mut p = Parser::new(input);
    p.parse_scalar_literal_text().is_ok() && {
        p.skip_ws();
        p.pos == input.len()
    }
}

pub(crate) fn parse_string_arg(input: &str) -> Result<String, ParseError> {
    let mut p = Parser::new(input);
    let value = p.parse_quoted()?;
    p.skip_ws();
    if p.pos == input.len() {
        Ok(value)
    } else {
        Err(syntax_error("expected string argument"))
    }
}

pub(crate) fn set_text(op: MetricBinarySetOp) -> &'static str {
    match op {
        MetricBinarySetOp::And => "and",
        MetricBinarySetOp::Or => "or",
        MetricBinarySetOp::Unless => "unless",
    }
}

pub(crate) fn strip_outer_metric_parentheses(input: &str) -> &str {
    let mut trimmed = input.trim();
    while let Some(inner) = outer_metric_parentheses_inner(trimmed) {
        if inner.len() >= trimmed.len() {
            break;
        }
        trimmed = inner.trim();
    }
    trimmed
}

#[must_use]
pub fn arithmetic_text(op: MetricScalarArithmeticOp) -> &'static str {
    match op {
        MetricScalarArithmeticOp::Add => "+",
        MetricScalarArithmeticOp::Subtract => "-",
        MetricScalarArithmeticOp::Multiply => "*",
        MetricScalarArithmeticOp::Divide => "/",
        MetricScalarArithmeticOp::Modulo => "%",
        MetricScalarArithmeticOp::Power => "^",
    }
}

pub(crate) fn comparison_text(op: ComparisonOp) -> &'static str {
    match op {
        ComparisonOp::Equal => "==",
        ComparisonOp::NotEqual => "!=",
        ComparisonOp::Greater => ">",
        ComparisonOp::GreaterEqual => ">=",
        ComparisonOp::Less => "<",
        ComparisonOp::LessEqual => "<=",
        ComparisonOp::RegexEqual => "=~",
        ComparisonOp::RegexNotEqual => "!~",
    }
}

/// Parse a complete, recursively nested `LogQL` expression.
///
/// # Errors
///
/// Returns an error when the expression is malformed or contains an unsupported leaf query.
pub fn parse_logql_expr(input: &str) -> Result<LogqlExpr, ParseError> {
    let expression = parse_expr(input.trim())?;
    if !matches!(expression, LogqlExpr::Variants { .. }) && expression.contains_variants() {
        return Err(syntax_error("variants must be a top-level expression"));
    }
    Ok(expression)
}

pub(crate) fn range_aggregation_supports_grouping(aggregation: &RangeAggregation) -> bool {
    matches!(
        aggregation,
        RangeAggregation::AvgOverTime
            | RangeAggregation::StdvarOverTime
            | RangeAggregation::StddevOverTime
            | RangeAggregation::QuantileOverTime(_)
            | RangeAggregation::MinOverTime
            | RangeAggregation::MaxOverTime
            | RangeAggregation::FirstOverTime
            | RangeAggregation::LastOverTime
    )
}

pub(crate) fn sign_is_unary_or_exponent(input: &str, at: usize) -> bool {
    let before_sign = input[..at].trim_end();
    let Some(last_char) = before_sign.chars().next_back() else {
        return true;
    };
    if matches!(
        last_char,
        '+' | '-' | '*' | '/' | '%' | '^' | '>' | '<' | '=' | '!'
    ) {
        return true;
    }
    if matches!(last_char, 'e' | 'E') {
        let mantissa = before_sign[..before_sign.len() - last_char.len_utf8()].trim_end();
        return mantissa
            .chars()
            .next_back()
            .is_some_and(|ch| ch.is_ascii_digit() || ch == '.');
    }
    false
}

// Expr Operator
#[derive(Clone, Copy)]
pub(crate) enum ExprOperator {
    Arithmetic(MetricScalarArithmeticOp),
    Comparison(ComparisonOp),
    Set(MetricBinarySetOp),
}

// Metric Binary Arithmetic
#[derive(Clone, Debug, PartialEq)]
pub struct MetricBinaryArithmetic {
    pub left: MetricQuery,
    pub op: MetricScalarArithmeticOp,
    pub matching: Option<MetricVectorMatching>,
    pub right: MetricQuery,
}

// Metric Binary Comparison
#[derive(Clone, Debug, PartialEq)]
pub struct MetricBinaryComparison {
    pub left: MetricQuery,
    pub op: ComparisonOp,
    pub bool_modifier: bool,
    pub matching: Option<MetricVectorMatching>,
    pub right: MetricQuery,
}

// Metric Binary Set
#[derive(Clone, Debug, PartialEq)]
pub struct MetricBinarySet {
    pub left: MetricQuery,
    pub op: MetricBinarySetOp,
    pub matching: Option<MetricVectorMatching>,
    pub right: MetricQuery,
}

// Metric Binary Set Op
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MetricBinarySetOp {
    And,
    Or,
    Unless,
}

// Metric Label Join
#[derive(Clone, Debug, PartialEq)]
pub struct MetricLabelJoin {
    pub query: MetricQuery,
    pub destination_label: String,
    pub separator: String,
    pub source_labels: Vec<String>,
}

// Metric Label Replace
#[derive(Clone, Debug, PartialEq)]
pub struct MetricLabelReplace {
    pub query: MetricQuery,
    pub destination_label: String,
    pub replacement: String,
    pub source_label: String,
    pub pattern: String,
}

// Metric Query
#[derive(Clone, Debug, PartialEq)]
pub struct MetricQuery {
    pub aggregation: RangeAggregation,
    pub vector_aggregation: Option<VectorAggregation>,
    pub range_grouping: Option<VectorGrouping>,
    pub stream: StreamQuery,
    pub range_ns: DurationNanos,
    pub offset_ns: OffsetNanos,
}

// Metric Scalar Arithmetic
#[derive(Clone, Debug, PartialEq)]
pub struct MetricScalarArithmetic {
    pub query: MetricQuery,
    pub op: MetricScalarArithmeticOp,
    pub scalar: String,
    pub scalar_on_left: bool,
}

// Metric Scalar Arithmetic Op
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MetricScalarArithmeticOp {
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,
    Power,
}

// Metric Scalar Comparison
#[derive(Clone, Debug, PartialEq)]
pub struct MetricScalarComparison {
    pub query: MetricQuery,
    pub op: ComparisonOp,
    pub bool_modifier: bool,
    pub scalar: String,
    pub scalar_on_left: bool,
}

// Metric Vector Group Modifier
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MetricVectorGroupModifier {
    Left(Vec<String>),
    Right(Vec<String>),
}

// Metric Vector Matching
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MetricVectorMatching {
    On {
        labels: Vec<String>,
        group: Option<MetricVectorGroupModifier>,
    },
    Ignoring {
        labels: Vec<String>,
        group: Option<MetricVectorGroupModifier>,
    },
}

// Quantile
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Quantile {
    pub numerator: QuantileNumerator,
    pub denominator: QuantileDenominator,
}

// Range Aggregation
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RangeAggregation {
    CountOverTime,
    Rate,
    RateCounter,
    BytesRate,
    BytesOverTime,
    AbsentOverTime,
    PresentOverTime,
    SumOverTime,
    AvgOverTime,
    StdvarOverTime,
    StddevOverTime,
    QuantileOverTime(Quantile),
    MinOverTime,
    MaxOverTime,
    FirstOverTime,
    LastOverTime,
}

// Range Aggregation Kind
pub(crate) enum RangeAggregationKind {
    Standard(RangeAggregation),
    QuantileOverTime,
}

// Vector Aggregation
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VectorAggregation {
    pub op: VectorAggregationOp,
    pub grouping: Option<VectorGrouping>,
}

// Vector Aggregation Op
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum VectorAggregationOp {
    Sum,
    Count,
    Min,
    Max,
    Avg,
    Stddev,
    Stdvar,
    CountValues(String),
    TopK(u64),
    BottomK(u64),
    ApproxTopK(u64),
    Sort,
    SortDesc,
}

// Vector Grouping
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum VectorGrouping {
    By(Vec<String>),
    Without(Vec<String>),
}

// Function Args
pub(crate) fn function_args<'a>(
    input: &'a str,
    name: &str,
) -> Result<Option<Vec<&'a str>>, ParseError> {
    let Some(rest) = input.strip_prefix(name) else {
        return Ok(None);
    };
    let rest = rest.trim_start();
    if !rest.starts_with('(') || !rest.ends_with(')') {
        return Ok(None);
    }
    let inner = &rest[1..rest.len() - 1];
    let mut starts = vec![0];
    let mut commas = Vec::new();
    scan_top_level(inner, |at| {
        if inner[at..].starts_with(',') {
            commas.push(at);
        }
    })?;
    starts.extend(commas.iter().map(|x| x + 1));
    let mut ends = commas;
    ends.push(inner.len());
    let mut args = Vec::with_capacity(starts.len());
    for index in 0..starts.len() {
        let start = starts[index];
        let end = ends[index];
        args.push(inner[start..end].trim());
    }
    Ok(Some(args))
}

// Operator At
#[derive(Clone, Copy)]
pub(crate) struct OperatorMatch {
    pub(crate) token_length: usize,
    pub(crate) operator: ExprOperator,
    pub(crate) precedence: u8,
}

struct WordOperatorCase {
    word: &'static str,
    op: MetricBinarySetOp,
    precedence: u8,
}

struct ComparisonOperatorCase {
    text: &'static str,
    op: ComparisonOp,
}

struct ArithmeticOperatorMatch {
    operator: MetricScalarArithmeticOp,
    precedence: u8,
}

pub(crate) fn operator_at(input: &str, at: usize) -> Option<OperatorMatch> {
    let rest = &input[at..];
    let boundary = |n: usize| {
        input[..at]
            .chars()
            .next_back()
            .is_none_or(|c| !is_ident_char(c))
            && input[at + n..]
                .chars()
                .next()
                .is_none_or(|c| !is_ident_char(c))
    };
    for case in [
        WordOperatorCase {
            word: "unless",
            op: MetricBinarySetOp::Unless,
            precedence: 2,
        },
        WordOperatorCase {
            word: "and",
            op: MetricBinarySetOp::And,
            precedence: 2,
        },
        WordOperatorCase {
            word: "or",
            op: MetricBinarySetOp::Or,
            precedence: 1,
        },
    ] {
        if rest.starts_with(case.word) && boundary(case.word.len()) {
            return Some(OperatorMatch {
                token_length: case.word.len(),
                operator: ExprOperator::Set(case.op),
                precedence: case.precedence,
            });
        }
    }
    for case in [
        ComparisonOperatorCase {
            text: ">=",
            op: ComparisonOp::GreaterEqual,
        },
        ComparisonOperatorCase {
            text: "<=",
            op: ComparisonOp::LessEqual,
        },
        ComparisonOperatorCase {
            text: "==",
            op: ComparisonOp::Equal,
        },
        ComparisonOperatorCase {
            text: "!=",
            op: ComparisonOp::NotEqual,
        },
        ComparisonOperatorCase {
            text: ">",
            op: ComparisonOp::Greater,
        },
        ComparisonOperatorCase {
            text: "<",
            op: ComparisonOp::Less,
        },
    ] {
        if rest.starts_with(case.text) {
            return Some(OperatorMatch {
                token_length: case.text.len(),
                operator: ExprOperator::Comparison(case.op),
                precedence: 3,
            });
        }
    }
    let ch = rest.chars().next()?;
    if matches!(ch, '+' | '-') && sign_is_unary_or_exponent(input, at) {
        return None;
    }
    let matched = match ch {
        '+' => ArithmeticOperatorMatch {
            operator: MetricScalarArithmeticOp::Add,
            precedence: 4,
        },
        '-' => ArithmeticOperatorMatch {
            operator: MetricScalarArithmeticOp::Subtract,
            precedence: 4,
        },
        '*' => ArithmeticOperatorMatch {
            operator: MetricScalarArithmeticOp::Multiply,
            precedence: 5,
        },
        '/' => ArithmeticOperatorMatch {
            operator: MetricScalarArithmeticOp::Divide,
            precedence: 5,
        },
        '%' => ArithmeticOperatorMatch {
            operator: MetricScalarArithmeticOp::Modulo,
            precedence: 5,
        },
        '^' => ArithmeticOperatorMatch {
            operator: MetricScalarArithmeticOp::Power,
            precedence: 6,
        },
        _ => return None,
    };
    Some(OperatorMatch {
        token_length: 1,
        operator: ExprOperator::Arithmetic(matched.operator),
        precedence: matched.precedence,
    })
}

// Parse Expr
struct LocatedOperator {
    start: usize,
    token_length: usize,
    operator: ExprOperator,
    precedence: u8,
}

pub(crate) fn parse_expr(input: &str) -> Result<LogqlExpr, ParseError> {
    let input = input.trim();
    if input.is_empty() {
        return Err(syntax_error("expected expression"));
    }
    let stream_error = if input.starts_with('{') {
        match parse_query(input) {
            Ok(query) => {
                return Ok(LogqlExpr::Stream {
                    query,
                    source: input.to_string(),
                });
            }
            Err(error) => Some(error),
        }
    } else {
        None
    };
    let mut candidate: Option<LocatedOperator> = None;
    if let Err(error) = scan_top_level(input, |at| {
        if let Some(found) = operator_at(input, at)
            && candidate.as_ref().is_none_or(|old| {
                found.precedence < old.precedence
                    || (found.precedence == old.precedence && found.precedence != 6)
            })
        {
            candidate = Some(LocatedOperator {
                start: at,
                token_length: found.token_length,
                operator: found.operator,
                precedence: found.precedence,
            });
        }
    }) {
        return Err(stream_error.unwrap_or(error));
    }
    if let Some(candidate) = candidate {
        let left = parse_expr(&input[..candidate.start])?;
        let mut parser = Parser::new(&input[candidate.start + candidate.token_length..]);
        let bool_modifier = matches!(candidate.operator, ExprOperator::Comparison(_))
            && parser.consume_keyword("bool");
        let matching = parser.parse_metric_vector_matching_modifier(!matches!(
            candidate.operator,
            ExprOperator::Set(_)
        ))?;
        let right_text = &parser.input[parser.pos..];
        let right = parse_expr(right_text)?;
        return Ok(match candidate.operator {
            ExprOperator::Arithmetic(op) => LogqlExpr::Arithmetic {
                left: Box::new(left),
                op,
                matching,
                right: Box::new(right),
            },
            ExprOperator::Comparison(op) => LogqlExpr::Comparison {
                left: Box::new(left),
                op,
                bool_modifier,
                matching,
                right: Box::new(right),
            },
            ExprOperator::Set(op) => LogqlExpr::Set {
                left: Box::new(left),
                op,
                matching,
                right: Box::new(right),
            },
        });
    }
    match parse_expr_primary(input) {
        Ok(expression) => Ok(expression),
        Err(error) => Err(stream_error.unwrap_or(error)),
    }
}

// Quoted
pub(crate) struct Quoted<'a>(pub(crate) &'a str);

impl fmt::Display for Quoted<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "\"")?;
        for c in self.0.chars() {
            match c {
                '\\' => write!(f, "\\\\")?,
                '"' => write!(f, "\\\"")?,
                '\n' => write!(f, "\\n")?,
                '\r' => write!(f, "\\r")?,
                '\t' => write!(f, "\\t")?,
                _ => write!(f, "{c}")?,
            }
        }
        write!(f, "\"")
    }
}

// Scan Top Level
pub(crate) fn scan_top_level(input: &str, mut found: impl FnMut(usize)) -> Result<(), ParseError> {
    let mut depth = 0usize;
    let mut quote = None;
    let mut escaped = false;
    let mut at = 0;
    while at < input.len() {
        let ch = input[at..].chars().next().expect("index is within input");
        if let Some(q) = quote {
            if escaped {
                escaped = false;
            } else if ch == '\\' && q == '"' {
                escaped = true;
            } else if ch == q {
                quote = None;
            }
            at += ch.len_utf8();
            continue;
        }
        match ch {
            '"' | '`' => quote = Some(ch),
            '(' | '[' | '{' => depth += 1,
            ')' | ']' | '}' => {
                depth = depth
                    .checked_sub(1)
                    .ok_or_else(|| syntax_error("unmatched closing delimiter"))?;
            }
            _ if depth == 0 => found(at),
            _ => {}
        }
        at += ch.len_utf8();
    }
    if quote.is_some() || depth != 0 {
        return Err(syntax_error("unterminated expression"));
    }
    Ok(())
}
