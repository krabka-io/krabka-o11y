//! Bounded, reproducible compositions checked against a live upstream oracle.

use std::{future::Future, path::Path};

use serde::Serialize;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

const SEED: u64 = 42;

/// Distinct result types for the bounded language grammars.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub enum QueryType {
    PromScalar,
    PromVector,
    PromRange,
    LogStream,
    LogVector,
    TracePredicate,
    ProfileSelector,
}

/// Label matcher operators shared by metric, log, and profile selectors.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub enum MatchOp {
    Eq,
    Neq,
    Regex,
    NotRegex,
}

/// A selector clause, with a quoted string value rather than embedded syntax.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct LabelMatcher {
    key: String,
    op: MatchOp,
    value: String,
}

impl LabelMatcher {
    pub fn new(key: &str, op: MatchOp, value: &str) -> Self {
        Self {
            key: key.to_owned(),
            op,
            value: value.to_owned(),
        }
    }
    fn render(&self) -> String {
        let op = match self.op {
            MatchOp::Eq => "=",
            MatchOp::Neq => "!=",
            MatchOp::Regex => "=~",
            MatchOp::NotRegex => "!~",
        };
        format!("{}{op}{}", self.key, quoted(&self.value))
    }
}

/// Typed field comparison operators.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub enum CompareOp {
    Eq,
    Neq,
    Gt,
    Gte,
    Lt,
    Lte,
    Regex,
    NotRegex,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
enum ScalarType {
    String,
    Duration,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct TraceField {
    scope: String,
    key: String,
    scalar_type: ScalarType,
}

impl TraceField {
    fn render(&self) -> String {
        if self.scope.is_empty() {
            if self.key.contains(':')
                || matches!(self.key.as_str(), "name" | "duration" | "status" | "kind")
            {
                self.key.clone()
            } else {
                format!(".{}", self.key)
            }
        } else {
            format!("{}.{}", self.scope, self.key)
        }
    }
}

/// An expression tree for typed fixture fields; source text is rendered last.
/// Attribute scalar types describe the populated fixture, rather than guessing
/// the types of arbitrary production data.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct TypedExpr {
    node: TypedNode,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "node", rename_all = "snake_case")]
enum TypedNode {
    PromScalar {
        value: i64,
    },
    PromVector {
        input: Box<TypedExpr>,
    },
    PromMetric {
        name: String,
        labels: Vec<LabelMatcher>,
    },
    PromRange {
        input: Box<TypedExpr>,
        seconds: u32,
    },
    PromRate {
        input: Box<TypedExpr>,
    },
    LogStream {
        labels: Vec<LabelMatcher>,
    },
    LogCount {
        input: Box<TypedExpr>,
        seconds: u32,
    },
    TraceString {
        field: TraceField,
        op: CompareOp,
        value: String,
    },
    TraceDuration {
        field: TraceField,
        op: CompareOp,
        nanos: i64,
    },
    TraceFieldComparison {
        left: TraceField,
        op: CompareOp,
        right: TraceField,
    },
    TraceScaledDurationComparison {
        left: TraceField,
        factor: i64,
        op: CompareOp,
        right: TraceField,
    },
    ProfileSelector {
        labels: Vec<LabelMatcher>,
    },
    Apply {
        constructor: Box<TypedConstructor>,
        input: Box<TypedExpr>,
    },
}

/// Constructors have explicit input/output types; no expression substitution.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub enum TypedConstructor {
    PromAbs,
    PromSum { by: Vec<String> },
    PromClampMin(i64),
    PromAdd(i64),
    LogSum,
    LogMax,
    LogAvg,
    LogAdd(i64),
    TraceAnd(TypedExpr),
    TraceOr(TypedExpr),
    ProfileAnd(LabelMatcher),
    Paren,
}

fn quoted(value: &str) -> String {
    serde_json::to_string(value).expect("string serialization")
}

fn valid_identifier(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b':' | b'.'))
        && !value.as_bytes()[0].is_ascii_digit()
}

fn valid_label_name(value: &str) -> bool {
    valid_identifier(value) && !value.contains([':', '.'])
}

fn valid_labels(labels: &[LabelMatcher]) -> bool {
    labels.iter().all(|label| valid_label_name(&label.key))
}

fn valid_trace_field(field: &TraceField) -> bool {
    if field.key.contains(':') {
        return field.scope.is_empty()
            && match field.scalar_type {
                ScalarType::String => matches!(
                    field.key.as_str(),
                    "span:name"
                        | "span:id"
                        | "event:name"
                        | "link:traceID"
                        | "link:spanID"
                        | "instrumentation:name"
                        | "instrumentation:version"
                ),
                ScalarType::Duration => {
                    matches!(field.key.as_str(), "span:duration" | "event:timeSinceStart")
                }
            };
    }
    if field.scalar_type == ScalarType::Duration {
        return field.scope.is_empty() && field.key == "duration";
    }
    valid_identifier(&field.key)
        && matches!(
            field.scope.as_str(),
            "" | "resource" | "span" | "event" | "link" | "instrumentation"
        )
        && !(field.scalar_type == ScalarType::String
            && matches!(field.scope.as_str(), "" | "span")
            && matches!(field.key.as_str(), "duration" | "status" | "kind"))
}

impl TypedExpr {
    pub fn prom_scalar(value: i64) -> Self {
        Self {
            node: TypedNode::PromScalar { value },
        }
    }
    pub fn prom_vector(input: Self) -> Self {
        Self {
            node: TypedNode::PromVector {
                input: Box::new(input),
            },
        }
    }
    pub fn prom_metric(name: &str, labels: &[LabelMatcher]) -> Self {
        Self {
            node: TypedNode::PromMetric {
                name: name.to_owned(),
                labels: labels.to_vec(),
            },
        }
    }
    pub fn prom_range(input: Self, seconds: u32) -> Self {
        Self {
            node: TypedNode::PromRange {
                input: Box::new(input),
                seconds,
            },
        }
    }
    pub fn prom_rate(input: Self) -> Self {
        Self {
            node: TypedNode::PromRate {
                input: Box::new(input),
            },
        }
    }
    pub fn log_count_over_time(labels: &[LabelMatcher], seconds: u32) -> Self {
        Self {
            node: TypedNode::LogCount {
                input: Box::new(Self {
                    node: TypedNode::LogStream {
                        labels: labels.to_vec(),
                    },
                }),
                seconds,
            },
        }
    }
    pub fn trace_string_compare(scope: &str, key: &str, op: CompareOp, value: &str) -> Self {
        Self {
            node: TypedNode::TraceString {
                field: TraceField {
                    scope: scope.to_owned(),
                    key: key.to_owned(),
                    scalar_type: ScalarType::String,
                },
                op,
                value: value.to_owned(),
            },
        }
    }
    pub fn trace_duration_compare(key: &str, op: CompareOp, nanos: i64) -> Self {
        Self {
            node: TypedNode::TraceDuration {
                field: TraceField {
                    scope: String::new(),
                    key: key.to_owned(),
                    scalar_type: ScalarType::Duration,
                },
                op,
                nanos,
            },
        }
    }
    pub fn trace_field_compare(
        scope: &str,
        key: &str,
        op: CompareOp,
        other_scope: &str,
        other_key: &str,
    ) -> Self {
        Self {
            node: TypedNode::TraceFieldComparison {
                left: TraceField {
                    scope: scope.to_owned(),
                    key: key.to_owned(),
                    scalar_type: ScalarType::String,
                },
                op,
                right: TraceField {
                    scope: other_scope.to_owned(),
                    key: other_key.to_owned(),
                    scalar_type: ScalarType::String,
                },
            },
        }
    }
    /// Compares fixture fields with the declared duration type, including intrinsics.
    pub fn trace_duration_field_compare(left: &str, op: CompareOp, right: &str) -> Self {
        Self {
            node: TypedNode::TraceFieldComparison {
                left: TraceField {
                    scope: String::new(),
                    key: left.into(),
                    scalar_type: ScalarType::Duration,
                },
                op,
                right: TraceField {
                    scope: String::new(),
                    key: right.into(),
                    scalar_type: ScalarType::Duration,
                },
            },
        }
    }
    /// Multiplies a duration field by an integer literal before comparing it.
    /// The mixed numeric operation yields a float, which is comparable to duration.
    pub fn trace_duration_times_integer_compare(
        left: &str,
        factor: i64,
        op: CompareOp,
        right: &str,
    ) -> Self {
        Self {
            node: TypedNode::TraceScaledDurationComparison {
                left: TraceField {
                    scope: String::new(),
                    key: left.into(),
                    scalar_type: ScalarType::Duration,
                },
                factor,
                op,
                right: TraceField {
                    scope: String::new(),
                    key: right.into(),
                    scalar_type: ScalarType::Duration,
                },
            },
        }
    }
    pub fn profile_selector(labels: &[LabelMatcher]) -> Self {
        Self {
            node: TypedNode::ProfileSelector {
                labels: labels.to_vec(),
            },
        }
    }
    fn value_type(&self) -> TestResult<QueryType> {
        let result = match &self.node {
            TypedNode::PromScalar { .. } => QueryType::PromScalar,
            TypedNode::PromVector { input } if input.value_type()? == QueryType::PromScalar => {
                QueryType::PromVector
            }
            TypedNode::PromMetric { name, labels }
                if valid_identifier(name) && !name.contains('.') && valid_labels(labels) =>
            {
                QueryType::PromVector
            }
            TypedNode::PromRange { input, seconds }
                if *seconds > 0 && input.value_type()? == QueryType::PromVector =>
            {
                QueryType::PromRange
            }
            TypedNode::PromRate { input } if input.value_type()? == QueryType::PromRange => {
                QueryType::PromVector
            }
            TypedNode::LogStream { labels } if !labels.is_empty() && valid_labels(labels) => {
                QueryType::LogStream
            }
            TypedNode::LogCount { input, seconds }
                if *seconds > 0 && input.value_type()? == QueryType::LogStream =>
            {
                QueryType::LogVector
            }
            TypedNode::TraceString { field, op, .. }
                if valid_trace_field(field)
                    && matches!(
                        op,
                        CompareOp::Eq | CompareOp::Neq | CompareOp::Regex | CompareOp::NotRegex
                    ) =>
            {
                QueryType::TracePredicate
            }
            TypedNode::TraceDuration { field, op, .. }
                if field.key == "duration"
                    && !matches!(op, CompareOp::Regex | CompareOp::NotRegex) =>
            {
                QueryType::TracePredicate
            }
            TypedNode::TraceFieldComparison { left, op, right }
                if valid_trace_field(left)
                    && valid_trace_field(right)
                    && left.scalar_type == right.scalar_type
                    && (matches!(op, CompareOp::Eq | CompareOp::Neq)
                        || (left.scalar_type == ScalarType::Duration
                            && !matches!(op, CompareOp::Regex | CompareOp::NotRegex))) =>
            {
                QueryType::TracePredicate
            }
            TypedNode::TraceScaledDurationComparison {
                left, op, right, ..
            } if valid_trace_field(left)
                && valid_trace_field(right)
                && left.scalar_type == ScalarType::Duration
                && right.scalar_type == ScalarType::Duration
                && !matches!(op, CompareOp::Regex | CompareOp::NotRegex) =>
            {
                QueryType::TracePredicate
            }
            TypedNode::ProfileSelector { labels } if !labels.is_empty() && valid_labels(labels) => {
                QueryType::ProfileSelector
            }
            TypedNode::Apply { constructor, input } => {
                constructor.output_type(input.value_type()?)?
            }
            _ => return Err("invalid typed query node or child signature".into()),
        };
        Ok(result)
    }
    fn render(&self) -> String {
        let labels = |items: &[LabelMatcher]| {
            items
                .iter()
                .map(LabelMatcher::render)
                .collect::<Vec<_>>()
                .join(",")
        };
        let comparison = |field: String, op: CompareOp, value: String| {
            let op = match op {
                CompareOp::Eq => "=",
                CompareOp::Neq => "!=",
                CompareOp::Gt => ">",
                CompareOp::Gte => ">=",
                CompareOp::Lt => "<",
                CompareOp::Lte => "<=",
                CompareOp::Regex => "=~",
                CompareOp::NotRegex => "!~",
            };
            format!("{field} {op} {value}")
        };
        match &self.node {
            TypedNode::PromScalar { value } => value.to_string(),
            TypedNode::PromVector { input } => format!("vector({})", input.render()),
            TypedNode::PromMetric {
                name,
                labels: items,
            } => {
                if items.is_empty() {
                    name.clone()
                } else {
                    format!("{name}{{{}}}", labels(items))
                }
            }
            TypedNode::PromRange { input, seconds } => {
                if matches!(input.node, TypedNode::PromMetric { .. }) {
                    format!("{}[{seconds}s]", input.render())
                } else {
                    format!("({})[{seconds}s:1s]", input.render())
                }
            }
            TypedNode::PromRate { input } => format!("rate({})", input.render()),
            TypedNode::LogStream { labels: items } => format!("{{{}}}", labels(items)),
            TypedNode::LogCount { input, seconds } => {
                format!("count_over_time({}[{seconds}s])", input.render())
            }
            TypedNode::TraceString { field, op, value } => {
                comparison(field.render(), *op, quoted(value))
            }
            TypedNode::TraceDuration { field, op, nanos } => {
                comparison(field.render(), *op, format!("{nanos}ns"))
            }
            TypedNode::TraceFieldComparison { left, op, right } => {
                comparison(left.render(), *op, right.render())
            }
            TypedNode::TraceScaledDurationComparison {
                left,
                factor,
                op,
                right,
            } => {
                let left = format!("{} * {factor}", left.render());
                comparison(format!("({left})"), *op, right.render())
            }
            TypedNode::ProfileSelector { labels: items } => labels(items),
            TypedNode::Apply { constructor, input } => constructor.render(&input.render()),
        }
    }
    fn children(&self) -> Vec<&Self> {
        match &self.node {
            TypedNode::PromVector { input }
            | TypedNode::PromRange { input, .. }
            | TypedNode::PromRate { input }
            | TypedNode::LogCount { input, .. } => vec![input],
            TypedNode::Apply { constructor, input } => match constructor.as_ref() {
                TypedConstructor::TraceAnd(other) | TypedConstructor::TraceOr(other) => {
                    vec![input, other]
                }
                _ => vec![input],
            },
            _ => Vec::new(),
        }
    }
    fn depth(&self) -> usize {
        if matches!(self.node, TypedNode::TraceScaledDurationComparison { .. }) {
            return 1;
        }
        self.children()
            .iter()
            .map(|child| child.depth() + 1)
            .max()
            .unwrap_or(0)
    }
    fn shrink_parents(&self, expected: QueryType, output: &mut Vec<String>) {
        for child in self.children() {
            child.shrink_parents(expected, output);
            if child.value_type().ok() == Some(expected) {
                let rendered = child.render();
                if !output.contains(&rendered) {
                    output.push(rendered);
                }
            }
        }
    }
}

impl TypedConstructor {
    fn output_type(&self, input: QueryType) -> TestResult<QueryType> {
        match self {
            Self::PromAbs | Self::PromClampMin(_) | Self::PromAdd(_)
                if input == QueryType::PromVector =>
            {
                Ok(input)
            }
            Self::PromSum { by }
                if input == QueryType::PromVector
                    && by.iter().all(|label| valid_label_name(label)) =>
            {
                Ok(input)
            }
            Self::LogSum | Self::LogMax | Self::LogAvg | Self::LogAdd(_)
                if input == QueryType::LogVector =>
            {
                Ok(input)
            }
            Self::TraceAnd(other) | Self::TraceOr(other)
                if input == QueryType::TracePredicate
                    && other.value_type()? == QueryType::TracePredicate =>
            {
                Ok(input)
            }
            Self::ProfileAnd(label)
                if input == QueryType::ProfileSelector && valid_label_name(&label.key) =>
            {
                Ok(input)
            }
            Self::Paren if input != QueryType::ProfileSelector => Ok(input),
            _ => Err("typed constructor input/output signature differs".into()),
        }
    }
    fn render(&self, input: &str) -> String {
        match self {
            Self::PromAbs => format!("abs({input})"),
            Self::PromSum { by } => {
                if by.is_empty() {
                    format!("sum({input})")
                } else {
                    format!("sum by({})({input})", by.join(","))
                }
            }
            Self::PromClampMin(min) => format!("clamp_min({input},{min})"),
            Self::PromAdd(value) | Self::LogAdd(value) => format!("({input})+{value}"),
            Self::LogSum => format!("sum({input})"),
            Self::LogMax => format!("max({input})"),
            Self::LogAvg => format!("avg({input})"),
            Self::TraceAnd(other) => format!("({input}) && ({})", other.render()),
            Self::TraceOr(other) => format!("({input}) || ({})", other.render()),
            Self::ProfileAnd(label) => format!("{input},{}", label.render()),
            Self::Paren => format!("({input})"),
        }
    }
}

/// Generate bounded, well-typed AST compositions and shrink only typed subtrees.
pub async fn run_typed<F, Fut>(
    language: &str,
    bases: &[TypedExpr],
    constructors: &[TypedConstructor],
    output: &Path,
    compare: F,
) -> TestResult
where
    F: FnMut(String) -> Fut,
    Fut: Future<Output = TestResult<Option<String>>>,
{
    let expected = match language {
        "promql" => QueryType::PromVector,
        "logql" => QueryType::LogVector,
        "traceql" | "traceql-field-comparisons" => QueryType::TracePredicate,
        "pyroscope" => QueryType::ProfileSelector,
        _ => return Err("unknown typed generator language".into()),
    };
    if bases.is_empty()
        || constructors.is_empty()
        || bases
            .iter()
            .any(|base| base.value_type().ok() != Some(expected) || base.depth() > 3)
    {
        return Err(
            "typed generator needs bases with the declared language type and depth <=3".into(),
        );
    }
    if constructors
        .iter()
        .any(|constructor| constructor.output_type(expected).ok() != Some(expected))
    {
        return Err("typed generator contains an incompatible constructor".into());
    }
    let mut state = SEED;
    let mut cases = Vec::new();
    for _ in 0..case_count()? {
        let mut expression = bases[next_index(&mut state, bases.len())].clone();
        for _ in 0..=next_index(&mut state, 3) {
            let applicable = constructors
                .iter()
                .filter(|constructor| constructor.output_type(expected).ok() == Some(expected))
                .filter_map(|constructor| {
                    let next = TypedExpr {
                        node: TypedNode::Apply {
                            constructor: Box::new(constructor.clone()),
                            input: Box::new(expression.clone()),
                        },
                    };
                    (next.depth() <= 3).then_some(next)
                })
                .collect::<Vec<_>>();
            if applicable.is_empty() {
                if expression.depth() == 3 {
                    break;
                }
                return Err("typed generator has no applicable constructor".into());
            }
            expression = applicable[next_index(&mut state, applicable.len())].clone();
        }
        let mut parents = Vec::new();
        expression.shrink_parents(expected, &mut parents);
        cases.push(Case {
            expression: expression.render(),
            parents,
            status: "not_run",
            details: None,
            reduced_expression: None,
            reduced_details: None,
            shrink_attempts: Vec::new(),
            typed_ast: Some(expression),
            value_type: Some(expected),
        });
    }
    evaluate_cases(language, output, cases, compare).await
}

#[derive(Serialize)]
struct Attempt {
    expression: String,
    status: &'static str,
    details: Option<String>,
}

#[derive(Serialize)]
struct Case {
    expression: String,
    parents: Vec<String>,
    status: &'static str,
    details: Option<String>,
    reduced_expression: Option<String>,
    reduced_details: Option<String>,
    shrink_attempts: Vec<Attempt>,
    typed_ast: Option<TypedExpr>,
    value_type: Option<QueryType>,
}

fn next_index(state: &mut u64, bound: usize) -> usize {
    *state = state
        .wrapping_mul(6_364_136_223_846_793_005)
        .wrapping_add(1_442_695_040_888_963_407);
    // Use high bits: low bits of this LCG repeat when grammar choices have
    // power-of-two sizes, starving otherwise valid branches.
    usize::try_from((*state >> 32) % u64::try_from(bound).expect("bounded fixture count fits u64"))
        .expect("remainder fits usize")
}

fn case_count() -> TestResult<usize> {
    let maximum = match std::env::var("KRABKA_GENERATED_NIGHTLY") {
        Ok(value) if value == "1" => 512,
        Ok(value) if value == "0" => 64,
        Err(std::env::VarError::NotPresent) => 64,
        _ => return Err("KRABKA_GENERATED_NIGHTLY must be 0 or 1".into()),
    };
    let count = match std::env::var("KRABKA_GENERATED_CASES") {
        Ok(value) => value.parse::<usize>()?,
        Err(std::env::VarError::NotPresent) => 16,
        Err(error) => return Err(error.into()),
    };
    if count == 0 || count > maximum {
        return Err(format!("KRABKA_GENERATED_CASES must be in 1..={maximum}").into());
    }
    Ok(count)
}

fn write_report(language: &str, output: &Path, cases: &[Case]) -> TestResult {
    std::fs::create_dir_all(output)?;
    std::fs::write(
        output.join(format!("{language}-generated-differential.json")),
        serde_json::to_vec_pretty(&serde_json::json!({
            "schema_version": 1,
            "language": language,
            "expected_outcome": if language.ends_with("-rejections") { "query-rejection" } else { "query-result" },
            "seed": SEED,
            "maximum_depth": 3,
            "generation_mode": if cases.iter().all(|case| case.typed_ast.is_some()) { "type-preserving-ast" } else { "unary-text-templates" },
            "shrinking": if cases.iter().all(|case| case.typed_ast.is_some()) { "same-result-type-ast-subtrees" } else { "text-template-ancestors" },
            "planned": cases.len(),
            "passed": cases.iter().filter(|case| case.status == "pass").count(),
            "mismatched": cases.iter().filter(|case| case.status == "mismatch").count(),
            "transport_errors": cases.iter().filter(|case| case.status == "transport_error").count(),
            "shrink_transport_errors": cases.iter().flat_map(|case| &case.shrink_attempts).filter(|attempt| attempt.status == "transport_error").count(),
            "not_run": cases.iter().filter(|case| case.status == "not_run").count(),
            "cases": cases,
        }))?,
    )?;
    Ok(())
}

/// Generates seed-42 compositions with one to three unary wrappers per case.
///
/// The caller supplies valid, positive bases/templates and returns a mismatch
/// description for semantic disagreement. Transport failures abort the run;
/// every planned case and every attempted reduction remains in the report.
/// `KRABKA_GENERATED_CASES` defaults to 16 and is bounded by 64, or 512 when
/// `KRABKA_GENERATED_NIGHTLY=1`. Output paths are explicit to avoid environment
/// mutation in tests; Docker callers pass `TEST_UNDECLARED_OUTPUTS_DIR`.
///
/// # Errors
///
/// Returns an error for invalid inputs, transport or report failures, or any
/// semantic mismatch after writing the complete case report.
pub async fn run<F, Fut>(
    language: &str,
    bases: &[&str],
    templates: &[&str],
    output: &Path,
    compare: F,
) -> TestResult
where
    F: FnMut(String) -> Fut,
    Fut: Future<Output = TestResult<Option<String>>>,
{
    run_cases(language, bases, templates, output, case_count()?, compare).await
}

fn generated_cases(
    language: &str,
    bases: &[&str],
    templates: &[&str],
    count: usize,
) -> TestResult<Vec<Case>> {
    if language.is_empty()
        || !language
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
        || bases.is_empty()
        || bases.iter().any(|base| base.trim().is_empty())
        || templates.is_empty()
        || templates
            .iter()
            .any(|template| template.matches("{expr}").count() != 1)
    {
        return Err(
            "generated differential needs a language, nonempty bases and unary templates".into(),
        );
    }
    let mut state = SEED;
    Ok((0..count)
        .map(|_| {
            let mut expression = bases[next_index(&mut state, bases.len())].to_owned();
            let mut parents = Vec::new();
            for _ in 0..=next_index(&mut state, 3) {
                parents.push(expression.clone());
                expression = templates[next_index(&mut state, templates.len())]
                    .replace("{expr}", &expression);
            }
            Case {
                expression,
                parents,
                status: "not_run",
                details: None,
                reduced_expression: None,
                reduced_details: None,
                shrink_attempts: Vec::new(),
                typed_ast: None,
                value_type: None,
            }
        })
        .collect())
}

async fn run_cases<F, Fut>(
    language: &str,
    bases: &[&str],
    templates: &[&str],
    output: &Path,
    count: usize,
    compare: F,
) -> TestResult
where
    F: FnMut(String) -> Fut,
    Fut: Future<Output = TestResult<Option<String>>>,
{
    let cases = generated_cases(language, bases, templates, count)?;
    evaluate_cases(language, output, cases, compare).await
}

async fn evaluate_cases<F, Fut>(
    language: &str,
    output: &Path,
    mut cases: Vec<Case>,
    mut compare: F,
) -> TestResult
where
    F: FnMut(String) -> Fut,
    Fut: Future<Output = TestResult<Option<String>>>,
{
    for index in 0..cases.len() {
        let result = compare(cases[index].expression.clone()).await;
        let mismatch = match result {
            Ok(mismatch) => mismatch,
            Err(error) => {
                cases[index].status = "transport_error";
                cases[index].details = Some(error.to_string());
                write_report(language, output, &cases)?;
                return Err(error);
            }
        };
        let Some(details) = mismatch else {
            cases[index].status = "pass";
            continue;
        };
        cases[index].status = "mismatch";
        cases[index].details = Some(details.clone());
        let mut reduced_expression = cases[index].expression.clone();
        let mut reduced_details = details;
        for parent in cases[index].parents.clone().into_iter().rev() {
            if parent.len() >= reduced_expression.len() {
                continue;
            }
            match compare(parent.clone()).await {
                Ok(details) => {
                    cases[index].shrink_attempts.push(Attempt {
                        expression: parent.clone(),
                        status: if details.is_some() {
                            "mismatch"
                        } else {
                            "pass"
                        },
                        details: details.clone(),
                    });
                    if let Some(details) = details {
                        reduced_expression = parent;
                        reduced_details = details;
                    }
                }
                Err(error) => {
                    cases[index].shrink_attempts.push(Attempt {
                        expression: parent,
                        status: "transport_error",
                        details: Some(error.to_string()),
                    });
                    cases[index].reduced_expression = Some(reduced_expression);
                    cases[index].reduced_details = Some(reduced_details);
                    write_report(language, output, &cases)?;
                    return Err(error);
                }
            }
        }
        cases[index].reduced_expression = Some(reduced_expression);
        cases[index].reduced_details = Some(reduced_details);
    }
    write_report(language, output, &cases)?;
    let mismatches = cases
        .iter()
        .filter(|case| case.status == "mismatch")
        .count();
    if mismatches > 0 {
        return Err(format!(
            "{language}: {mismatches} generated mismatches; seed {SEED}; report in {}",
            output.display()
        )
        .into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    fn apply(input: TypedExpr, constructor: TypedConstructor) -> TypedExpr {
        TypedExpr {
            node: TypedNode::Apply {
                constructor: Box::new(constructor),
                input: Box::new(input),
            },
        }
    }

    #[test]
    fn bounded_grammars_validate_signatures_and_quote_literal_values() {
        let labels = [
            LabelMatcher::new("app", MatchOp::Eq, "checkout"),
            LabelMatcher::new("env", MatchOp::Neq, "missing"),
            LabelMatcher::new("region", MatchOp::Regex, "us.*"),
            LabelMatcher::new("missing", MatchOp::NotRegex, ".+"),
        ];
        let metric = TypedExpr::prom_metric("request_total", &labels);
        assert!(metric.value_type().unwrap() == QueryType::PromVector);
        let range = TypedExpr::prom_range(metric.clone(), 60);
        assert!(range.value_type().unwrap() == QueryType::PromRange);
        let rate = TypedExpr::prom_rate(range);
        assert!(rate.value_type().unwrap() == QueryType::PromVector);
        assert!(rate.render().starts_with("rate(request_total{"));
        let scalar = TypedExpr::prom_scalar(1);
        assert!(scalar.value_type().unwrap() == QueryType::PromScalar);
        let vector = TypedExpr::prom_vector(scalar);
        assert!(vector.render() == "vector(1)");
        assert!(TypedExpr::prom_range(vector, 60).render() == "(vector(1))[60s:1s]");
        for constructor in [
            TypedConstructor::PromAbs,
            TypedConstructor::PromSum { by: vec![] },
            TypedConstructor::PromSum {
                by: vec!["app".into()],
            },
            TypedConstructor::PromClampMin(0),
            TypedConstructor::PromAdd(1),
            TypedConstructor::Paren,
        ] {
            let expression = apply(metric.clone(), constructor);
            assert!(expression.value_type().unwrap() == QueryType::PromVector);
            assert!(expression.depth() == 1);
        }
        let log = TypedExpr::log_count_over_time(&labels, 60);
        assert!(log.children()[0].value_type().unwrap() == QueryType::LogStream);
        for constructor in [
            TypedConstructor::LogSum,
            TypedConstructor::LogMax,
            TypedConstructor::LogAvg,
            TypedConstructor::LogAdd(1),
        ] {
            assert!(apply(log.clone(), constructor).value_type().unwrap() == QueryType::LogVector);
        }
        for op in [
            CompareOp::Eq,
            CompareOp::Neq,
            CompareOp::Gt,
            CompareOp::Gte,
            CompareOp::Lt,
            CompareOp::Lte,
        ] {
            assert!(
                TypedExpr::trace_duration_compare("duration", op, 1)
                    .value_type()
                    .unwrap()
                    == QueryType::TracePredicate
            );
        }
        for op in [
            CompareOp::Eq,
            CompareOp::Neq,
            CompareOp::Regex,
            CompareOp::NotRegex,
        ] {
            let trace = TypedExpr::trace_string_compare("resource", "service.name", op, "checkout");
            for constructor in [
                TypedConstructor::TraceAnd(trace.clone()),
                TypedConstructor::TraceOr(trace.clone()),
            ] {
                assert!(
                    apply(trace.clone(), constructor).value_type().unwrap()
                        == QueryType::TracePredicate
                );
            }
        }
        let fields = TypedExpr::trace_field_compare("", "foo", CompareOp::Eq, "", "bar");
        assert!(fields.render() == ".foo = .bar");
        assert!(fields.value_type().unwrap() == QueryType::TracePredicate);
        let intrinsics =
            TypedExpr::trace_field_compare("", "span:name", CompareOp::Eq, "", "event:name");
        assert!(
            intrinsics.value_type().unwrap() == QueryType::TracePredicate
                && intrinsics.render() == "span:name = event:name"
        );
        let durations = TypedExpr::trace_duration_field_compare(
            "span:duration",
            CompareOp::Gt,
            "event:timeSinceStart",
        );
        assert!(
            durations.value_type().unwrap() == QueryType::TracePredicate
                && durations.render() == "span:duration > event:timeSinceStart"
        );
        let scaled = TypedExpr::trace_duration_times_integer_compare(
            "duration",
            2,
            CompareOp::Gt,
            "duration",
        );
        assert!(
            scaled.value_type().unwrap() == QueryType::TracePredicate
                && scaled.render() == "(duration * 2) > duration"
                && scaled.depth() == 1
        );
        assert!(
            TypedExpr::trace_duration_field_compare("name", CompareOp::Gt, "duration")
                .value_type()
                .is_err()
        );
        assert!(
            TypedExpr::trace_duration_field_compare("duration", CompareOp::Regex, "duration")
                .value_type()
                .is_err()
        );
        assert!(
            TypedExpr::trace_duration_times_integer_compare(
                "span:name",
                2,
                CompareOp::Gt,
                "duration"
            )
            .value_type()
            .is_err()
        );
        assert!(
            TypedExpr::trace_field_compare("", "span:unknown", CompareOp::Eq, "", "event:name")
                .value_type()
                .is_err()
        );
        let selector = TypedExpr::profile_selector(&labels);
        let profile = apply(
            selector,
            TypedConstructor::ProfileAnd(LabelMatcher::new("new", MatchOp::Eq, "value")),
        );
        assert!(profile.value_type().unwrap() == QueryType::ProfileSelector);
        assert!(profile.render().ends_with(",new=\"value\""));
        let literal = TypedExpr::trace_string_compare("", "name", CompareOp::Eq, "x\" || true");
        assert!(literal.render() == "name = \"x\\\" || true\"");
        assert!(TypedExpr::prom_rate(log.clone()).value_type().is_err());
        assert!(TypedExpr::prom_vector(metric.clone()).value_type().is_err());
        assert!(
            apply(metric, TypedConstructor::LogSum)
                .value_type()
                .is_err()
        );
        assert!(
            TypedExpr::trace_string_compare("", "duration", CompareOp::Eq, "x")
                .value_type()
                .is_err()
        );
        assert!(
            TypedExpr::trace_field_compare("", "duration", CompareOp::Eq, "", "foo")
                .value_type()
                .is_err()
        );
        assert!(
            TypedExpr::trace_string_compare("wrong", "name", CompareOp::Eq, "x")
                .value_type()
                .is_err()
        );
        assert!(
            TypedExpr::trace_duration_compare("duration", CompareOp::Regex, 1)
                .value_type()
                .is_err()
        );
        assert!(
            TypedExpr::prom_metric("bad.name", &[])
                .value_type()
                .is_err()
        );
        assert!(
            TypedExpr::profile_selector(&[LabelMatcher::new("bad.key", MatchOp::Eq, "x")])
                .value_type()
                .is_err()
        );
    }

    #[tokio::test]
    async fn typed_generation_saves_ast_and_reduces_a_real_mismatch_to_same_type_subtree() {
        let output = tempfile::tempdir().unwrap();
        let result = run_typed(
            "promql",
            &[TypedExpr::prom_metric("probe_metric", &[])],
            &[TypedConstructor::PromAbs],
            output.path(),
            |expression| async move {
                Ok(expression
                    .starts_with("abs(")
                    .then(|| "independent oracle value differs".to_owned()))
            },
        )
        .await;
        assert!(result.is_err());
        let report: serde_json::Value = serde_json::from_slice(
            &std::fs::read(output.path().join("promql-generated-differential.json")).unwrap(),
        )
        .unwrap();
        assert!(report["generation_mode"] == "type-preserving-ast");
        assert!(report["shrinking"] == "same-result-type-ast-subtrees");
        for case in report["cases"].as_array().unwrap() {
            assert!(case["typed_ast"].is_object());
            assert!(case["value_type"] == "PromVector");
            assert!(case["reduced_expression"] == "abs(probe_metric)");
            assert!(
                case["shrink_attempts"].as_array().unwrap().last().unwrap()["expression"]
                    == "probe_metric"
            );
        }
        let invalid = run_typed(
            "promql",
            &[TypedExpr::prom_metric("probe_metric", &[])],
            &[TypedConstructor::LogSum],
            output.path(),
            |_| async { Ok(None) },
        )
        .await;
        assert!(invalid.is_err());
    }

    #[tokio::test]
    async fn a_known_semantic_mismatch_is_reduced_and_saved() {
        let output = tempfile::tempdir().unwrap();
        let result = run_cases(
            "promql",
            &["probe_metric"],
            &["abs({expr})"],
            output.path(),
            4,
            |expression| async move {
                Ok(expression
                    .starts_with("abs(")
                    .then(|| format!("oracle value differs for {expression}")))
            },
        )
        .await;
        assert!(result.is_err());
        let report: serde_json::Value = serde_json::from_slice(
            &std::fs::read(output.path().join("promql-generated-differential.json")).unwrap(),
        )
        .unwrap();
        assert!(report["seed"] == SEED);
        assert!(report["planned"] == 4);
        assert!(report["mismatched"] == 4);
        assert!(report["not_run"] == 0);
        let cases = report["cases"].as_array().unwrap();
        assert!(cases.len() == 4);
        assert!(
            cases
                .iter()
                .any(|case| case["expression"] != case["reduced_expression"])
        );
        for case in cases {
            assert!(case["reduced_expression"] == "abs(probe_metric)");
            assert!(case["reduced_details"] == "oracle value differs for abs(probe_metric)");
            let attempts = case["shrink_attempts"].as_array().unwrap();
            assert!(attempts.last().unwrap()["expression"] == "probe_metric");
            assert!(attempts.last().unwrap()["status"] == "pass");
        }
    }
}
