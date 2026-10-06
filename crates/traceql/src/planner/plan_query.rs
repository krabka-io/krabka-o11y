use super::{
    FieldExpr, MatchScope, PlannedSpanset, PlannerContext, Query, Result, ScanOptions, Scope,
    SpanStore, SpansetExpr, nested_projection_matcher, plan_spanset_sql, selector,
};

pub(crate) async fn plan_query<S: SpanStore>(
    store: &S,
    ctx: &PlannerContext,
    q: &Query,
) -> Result<PlannedSpanset> {
    // A single conjunction is evaluated against packed attribute values by
    // the store and needs no SQL attribute columns.
    if ctx.scan_options.sample_fraction.is_none()
        && ctx.scan_options.trace_sample_fraction.is_none()
        && q.pipeline.is_empty()
        && let SpansetExpr::Selector(fields) = &q.root
        && !fields.has_field_comparison()
        && !selector::has_nested_scope(fields)
        && !selector::has_parent_scope(fields)
        && selector::field_expr_to_matcher_disjuncts(fields)
            .is_some_and(|disjuncts| disjuncts.len() == 1)
    {
        return selector::plan_selector(store, ctx, fields).await;
    }
    // Projection dependencies cross OR/NOT and structural branches; pushing
    // their predicates into the scan would discard spans needed by siblings.
    let mut options = ctx.scan_options.clone();
    project_spanset_fields(&q.root, &mut options);
    let ctx = &PlannerContext {
        tenant: ctx.tenant.clone(),
        start_ns: ctx.start_ns,
        end_ns: ctx.end_ns,
        scan_options: options,
    };
    if !q.pipeline.is_empty()
        || ctx.scan_options.sample_fraction.is_some()
        || ctx.scan_options.trace_sample_fraction.is_some()
    {
        return plan_spanset_sql(store, ctx, &q.root, &q.pipeline).await;
    }
    match &q.root {
        SpansetExpr::Selector(fe) => selector::plan_selector(store, ctx, fe).await,
        SpansetExpr::And(_, _) | SpansetExpr::Or(_, _) | SpansetExpr::Structural { .. } => {
            plan_spanset_sql(store, ctx, &q.root, &[]).await
        }
    }
}

fn project_spanset_fields(expr: &SpansetExpr, options: &mut ScanOptions) {
    match expr {
        SpansetExpr::Selector(fields) => project_selector_fields(fields, options),
        SpansetExpr::And(lhs, rhs)
        | SpansetExpr::Or(lhs, rhs)
        | SpansetExpr::Structural { lhs, rhs, .. } => {
            project_spanset_fields(lhs, options);
            project_spanset_fields(rhs, options);
        }
    }
}

fn project_selector_fields(expr: &FieldExpr, options: &mut ScanOptions) {
    options.include_raw_attributes |= expr.has_field_comparison();
    match expr {
        FieldExpr::ExpressionComparison { lhs, rhs, .. } => {
            options.include_raw_attributes = true;
            let mut fields = Vec::new();
            lhs.collect_fields(&mut fields);
            rhs.collect_fields(&mut fields);
            for field in fields {
                project_selector_fields(&FieldExpr::Field(field.clone()), options);
            }
        }
        FieldExpr::FieldComparison { lhs, rhs, .. } => {
            options.include_raw_attributes = true;
            project_selector_fields(&FieldExpr::Field(lhs.clone()), options);
            project_selector_fields(&FieldExpr::Field(rhs.clone()), options);
        }
        // Nested fields expand event/link rows using their predicates. Keep
        // those dependencies local to each selector disjunct, rather than
        // combining mutually exclusive branches into one scan predicate.
        FieldExpr::Comparison { .. } if selector::has_nested_scope(expr) => {}
        FieldExpr::Comparison { .. } => {
            for mut matcher in selector::field_expr_to_matchers(expr) {
                if matcher.scope == MatchScope::Parent {
                    matcher.scope = MatchScope::Both;
                }
                if !options.projection_matchers.contains(&matcher) {
                    options.projection_matchers.push(matcher);
                }
            }
        }
        FieldExpr::Field(field) => {
            let mut field = field.clone();
            if field.scope == Scope::Parent {
                field.scope = Scope::Both;
            }
            if let Some(matcher) = nested_projection_matcher(&field)
                && !options.projection_matchers.contains(&matcher)
            {
                options.projection_matchers.push(matcher);
            }
            if field.scope == Scope::Both {
                for scope in [Scope::Event, Scope::Link, Scope::Instrumentation] {
                    project_selector_fields(
                        &FieldExpr::Field(crate::ast::Field {
                            scope,
                            key: field.key.clone(),
                        }),
                        options,
                    );
                }
            }
        }
        FieldExpr::And(lhs, rhs) | FieldExpr::Or(lhs, rhs) => {
            project_selector_fields(lhs, options);
            project_selector_fields(rhs, options);
        }
        FieldExpr::Not(inner) => project_selector_fields(inner, options),
        FieldExpr::Const(_) => {}
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::{MatchValue, parse};

    #[test]
    fn structural_and_boolean_branches_keep_all_typed_projection_dependencies() {
        let query = parse(r#"{ (.route = "root" || .missing = nil) && !(.slow = true) } >> { parent.route = "root" && .cost > 3 && .ratio < 1.5 }"#).unwrap();
        let mut options = ScanOptions::default();
        project_spanset_fields(&query.root, &mut options);
        let dependencies = options
            .projection_matchers
            .iter()
            .map(|matcher| (matcher.scope, matcher.key.as_str(), &matcher.value))
            .collect::<Vec<_>>();
        assert!(
            dependencies
                == vec![
                    (MatchScope::Both, "route", &MatchValue::Str("root".into())),
                    (MatchScope::Both, "missing", &MatchValue::Nil),
                    (MatchScope::Both, "slow", &MatchValue::Bool(true)),
                    (MatchScope::Both, "cost", &MatchValue::Int(3)),
                    (MatchScope::Both, "ratio", &MatchValue::Float(1.5)),
                ]
        );
        // The same regular OR has no safe conjunctive scan filter.
        let query = parse(r#"{ .route = "root" || .cost > 3 }"#).unwrap();
        let SpansetExpr::Selector(fields) = query.root else {
            panic!("selector");
        };
        assert!(selector::field_expr_to_matchers(&fields).is_empty());
    }
}
