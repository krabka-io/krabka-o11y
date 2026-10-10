use std::{collections::BTreeMap, sync::Arc};

use promql_parser::parser::{Expr, Extension, value::ValueType};

use super::leaf_extension_expr::{LeafExtension, LeafExtensionExpr};
use crate::{PromqlMatcher, PromqlString};

/// A selector with byte-valued matchers. The dependency child retains only its
/// JSON view; all execution uses the separately typed matcher sets.
#[derive(Clone, Debug)]
pub(crate) struct ByteSelectorExpr {
    pub(crate) child: Expr,
    pub(crate) matcher_sets: Vec<Vec<PromqlMatcher>>,
}

impl LeafExtension for ByteSelectorExpr {
    const NAME: &'static str = "byteSelector";

    fn leaf_value_type(&self) -> ValueType {
        self.child.value_type()
    }
}

pub(crate) fn restore_byte_selector(expr: &mut Expr, values: &BTreeMap<String, PromqlString>) {
    let selector = match expr {
        Expr::VectorSelector(selector) => selector,
        Expr::MatrixSelector(selector) => &mut selector.vs,
        _ => return,
    };
    let groups = if selector.matchers.or_matchers.is_empty() {
        vec![&selector.matchers.matchers]
    } else {
        selector.matchers.or_matchers.iter().collect()
    };
    if !groups.iter().any(|matchers| {
        matchers
            .iter()
            .any(|matcher| values.contains_key(&matcher.value))
    }) {
        return;
    }
    let matcher_sets = groups
        .into_iter()
        .map(|matchers| {
            let mut result = Vec::new();
            if let Some(name) = &selector.name {
                result.push(PromqlMatcher::new(
                    "__name__",
                    krabka_blockstore::MatchOp::Eq,
                    name,
                ));
            }
            for matcher in matchers {
                let op = match matcher.op {
                    promql_parser::label::MatchOp::Equal => krabka_blockstore::MatchOp::Eq,
                    promql_parser::label::MatchOp::NotEqual => krabka_blockstore::MatchOp::Neq,
                    promql_parser::label::MatchOp::Re(_) => krabka_blockstore::MatchOp::Re,
                    promql_parser::label::MatchOp::NotRe(_) => krabka_blockstore::MatchOp::Nre,
                };
                result.push(PromqlMatcher::new(
                    &matcher.name,
                    op,
                    values
                        .get(&matcher.value)
                        .cloned()
                        .unwrap_or_else(|| matcher.value.clone().into()),
                ));
            }
            result
        })
        .collect();
    for matcher in selector
        .matchers
        .matchers
        .iter_mut()
        .chain(selector.matchers.or_matchers.iter_mut().flatten())
    {
        if let Some(value) = values.get(&matcher.value) {
            matcher.value = value.as_str().to_owned();
        }
    }
    let child = expr.clone();
    *expr = Expr::Extension(Extension {
        expr: Arc::new(LeafExtensionExpr(ByteSelectorExpr {
            child,
            matcher_sets,
        })),
    });
}

impl ByteSelectorExpr {
    pub(crate) fn format(&self) -> String {
        let (mut selector, range) = match &self.child {
            Expr::VectorSelector(selector) => (selector.clone(), String::new()),
            Expr::MatrixSelector(selector) => (
                selector.vs.clone(),
                format!(
                    "[{}]",
                    promql_parser::util::display_duration(&selector.range)
                ),
            ),
            _ => unreachable!("byte selector has a selector child"),
        };
        let metric_name = selector.name.clone();
        let mut name_selector = selector.clone();
        name_selector.matchers.matchers.clear();
        name_selector.matchers.or_matchers.clear();
        name_selector.offset = None;
        name_selector.at = None;
        let name_text = name_selector.to_string();
        let groups = self
            .matcher_sets
            .iter()
            .map(|matchers| {
                matchers
                    .iter()
                    .filter(|matcher| {
                        !(matcher.name == "__name__"
                            && matcher.op == krabka_blockstore::MatchOp::Eq
                            && metric_name
                                .as_ref()
                                .is_some_and(|name| matcher.value == *name))
                    })
                    .map(|matcher| {
                        let name = if matcher.name.bytes().enumerate().all(|(index, byte)| {
                            byte == b'_'
                                || byte.is_ascii_alphabetic()
                                || (index > 0 && byte.is_ascii_digit())
                        }) && !matcher.name.is_empty()
                        {
                            matcher.name.clone()
                        } else {
                            serde_json::to_string(&matcher.name).expect("label name")
                        };
                        let op = match matcher.op {
                            krabka_blockstore::MatchOp::Eq => "=",
                            krabka_blockstore::MatchOp::Neq => "!=",
                            krabka_blockstore::MatchOp::Re => "=~",
                            krabka_blockstore::MatchOp::Nre => "!~",
                        };
                        format!(
                            "{name}{op}{}",
                            krabka_logql::quote_go_bytes(matcher.value.as_bytes())
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(", ")
            })
            .collect::<Vec<_>>()
            .join(" or ");
        selector.name = Some("byte_selector_suffix".to_owned());
        selector.matchers.matchers.clear();
        selector.matchers.or_matchers.clear();
        let suffix = selector.to_string();
        let selector_text = if metric_name.is_some() {
            if let Some(quoted_name) = name_text
                .strip_prefix('{')
                .and_then(|text| text.strip_suffix('}'))
            {
                format!("{{{quoted_name}, {groups}}}")
            } else {
                format!("{name_text}{{{groups}}}")
            }
        } else {
            format!("{{{groups}}}")
        };
        format!(
            "{selector_text}{range}{}",
            suffix.trim_start_matches("byte_selector_suffix")
        )
    }
}
