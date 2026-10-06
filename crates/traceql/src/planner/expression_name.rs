use crate::ast::{ArithmeticOp, ComparisonOp, Field, FieldExpr, ScalarExpr, Scope, Value};

pub(super) fn field_name(field: &Field) -> String {
    let prefix = match field.scope {
        Scope::Both => ".",
        Scope::Span => "span.",
        Scope::Resource => "resource.",
        Scope::Event => "event.",
        Scope::Link => "link.",
        Scope::Instrumentation => "instrumentation.",
        Scope::Parent => "parent.",
        Scope::Intrinsic(_) => "",
    };
    format!("{prefix}{}", field.key)
}

pub(super) fn scalar_name(expr: &ScalarExpr) -> String {
    match expr {
        ScalarExpr::Field(field) => field_name(field),
        ScalarExpr::Literal(value) => value_name(value),
        ScalarExpr::Predicate(expr) => predicate_name(expr),
        ScalarExpr::Negate(inner) => format!("-{}", wrap_scalar(inner)),
        ScalarExpr::Binary { lhs, op, rhs } => format!(
            "{} {} {}",
            wrap_scalar(lhs),
            match op {
                ArithmeticOp::Add => "+",
                ArithmeticOp::Sub => "-",
                ArithmeticOp::Mul => "*",
                ArithmeticOp::Div => "/",
                ArithmeticOp::Mod => "%",
                ArithmeticOp::Pow => "^",
            },
            wrap_scalar(rhs)
        ),
    }
}

fn wrap_scalar(expr: &ScalarExpr) -> String {
    let name = scalar_name(expr);
    if matches!(expr, ScalarExpr::Field(_) | ScalarExpr::Literal(_)) {
        name
    } else {
        format!("({name})")
    }
}

fn value_name(value: &Value) -> String {
    match value {
        Value::Str(value) => format!("`{value}`"),
        Value::Float(value) => format!("{value:?}"),
        Value::Int(value) => value.to_string(),
        Value::Duration(value) => crate::engine::metric_duration_label(*value),
        Value::Bool(value) => value.to_string(),
        Value::Nil => "nil".into(),
        Value::Array(values) => format!(
            "[{}]",
            values.iter().map(value_name).collect::<Vec<_>>().join(", ")
        ),
    }
}

fn comparison_name(op: ComparisonOp) -> &'static str {
    match op {
        ComparisonOp::Eq => "=",
        ComparisonOp::Neq => "!=",
        ComparisonOp::Gt => ">",
        ComparisonOp::Gte => ">=",
        ComparisonOp::Lt => "<",
        ComparisonOp::Lte => "<=",
        ComparisonOp::Re => "=~",
        ComparisonOp::Nre => "!~",
    }
}

fn wrap_predicate(expr: &FieldExpr) -> String {
    let name = predicate_name(expr);
    if matches!(expr, FieldExpr::Field(_) | FieldExpr::Const(_)) {
        name
    } else {
        format!("({name})")
    }
}

fn predicate_name(expr: &FieldExpr) -> String {
    match expr {
        FieldExpr::Field(field) => field_name(field),
        FieldExpr::Const(value) => value.to_string(),
        FieldExpr::Comparison { lhs, op, rhs } => format!(
            "{} {} {}",
            field_name(lhs),
            comparison_name(*op),
            value_name(rhs)
        ),
        FieldExpr::FieldComparison { lhs, op, rhs } => format!(
            "{} {} {}",
            field_name(lhs),
            comparison_name(*op),
            field_name(rhs)
        ),
        FieldExpr::ExpressionComparison { lhs, op, rhs } => format!(
            "{} {} {}",
            wrap_scalar(lhs),
            comparison_name(*op),
            wrap_scalar(rhs)
        ),
        FieldExpr::And(lhs, rhs) => format!("{} && {}", wrap_predicate(lhs), wrap_predicate(rhs)),
        FieldExpr::Or(lhs, rhs) => format!("{} || {}", wrap_predicate(lhs), wrap_predicate(rhs)),
        FieldExpr::Not(inner) => format!("!{}", wrap_predicate(inner)),
    }
}
