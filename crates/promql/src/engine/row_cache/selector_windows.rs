use krabka_units::prelude::*;
use promql_parser::parser::{Expr, VectorSelector};

use super::{HashMap, matchers_cache_key, range_scan_cache_inner::TimeWindow};
use crate::{
    PromqlMatcher,
    engine::selector::{apply_selector_time_modifier, label_matcher_sets, selector_duration},
    error::Result,
    planner::{
        ExtendedSelectorExpr, ExtendedSelectorModifier, byte_selector_expr::ByteSelectorExpr,
    },
};

pub(crate) fn selector_windows(
    expr: &Expr,
    start_ms: i64,
    end_ms: i64,
    step: Time,
    lookback_ms: i64,
) -> Result<Option<HashMap<String, TimeWindow>>> {
    let mut windows = SelectorWindows {
        grid: TimeWindow { start_ms, end_ms },
        lookback_ms,
        step,
        cacheable: true,
        unions: HashMap::new(),
    };
    windows.collect(expr, None, None)?;
    Ok(windows.cacheable.then_some(windows.unions))
}

struct SelectorWindows {
    grid: TimeWindow,
    lookback_ms: i64,
    step: Time,
    cacheable: bool,
    unions: HashMap<String, TimeWindow>,
}

impl SelectorWindows {
    fn collect(
        &mut self,
        expr: &Expr,
        modifier: Option<ExtendedSelectorModifier>,
        typed_matchers: Option<&[Vec<PromqlMatcher>]>,
    ) -> Result<()> {
        match expr {
            Expr::VectorSelector(selector) => {
                if modifier == Some(ExtendedSelectorModifier::Smoothed) {
                    self.insert(selector, 0, modifier, typed_matchers)?;
                } else {
                    self.insert(selector, self.lookback_ms, None, typed_matchers)?;
                }
            }
            Expr::MatrixSelector(selector) => {
                self.insert(
                    &selector.vs,
                    selector_duration(selector.range)?.millis_i64(),
                    modifier,
                    typed_matchers,
                )?;
            }
            Expr::Aggregate(aggregate) => {
                self.collect(&aggregate.expr, modifier, typed_matchers)?;
                if let Some(param) = &aggregate.param {
                    self.collect(param, modifier, typed_matchers)?;
                }
            }
            Expr::Unary(unary) => self.collect(&unary.expr, modifier, typed_matchers)?,
            Expr::Paren(paren) => self.collect(&paren.expr, modifier, typed_matchers)?,
            Expr::Binary(binary) => {
                self.collect(&binary.lhs, modifier, typed_matchers)?;
                self.collect(&binary.rhs, modifier, typed_matchers)?;
            }
            Expr::Call(call) => {
                for arg in &call.args.args {
                    self.collect(arg, modifier, typed_matchers)?;
                }
            }
            Expr::Extension(extension) => {
                if let Some(bytes) = extension.expr.as_any().downcast_ref::<ByteSelectorExpr>() {
                    self.collect(&bytes.child, modifier, Some(&bytes.matcher_sets))?;
                } else {
                    let modifier = extension
                        .expr
                        .as_any()
                        .downcast_ref::<ExtendedSelectorExpr>()
                        .map(ExtendedSelectorExpr::modifier)
                        .or(modifier);
                    for child in extension.expr.children() {
                        self.collect(child, modifier, typed_matchers)?;
                    }
                }
            }
            // The nested range driver derives windows for its own grid.
            Expr::Subquery(_) | Expr::NumberLiteral(_) | Expr::StringLiteral(_) => {}
        }
        Ok(())
    }

    fn insert(
        &mut self,
        selector: &VectorSelector,
        range_ms: i64,
        modifier: Option<ExtendedSelectorModifier>,
        typed_matchers: Option<&[Vec<PromqlMatcher>]>,
    ) -> Result<()> {
        let start_ms = apply_selector_time_modifier(
            self.grid.start_ms,
            selector.at.as_ref(),
            selector.offset.as_ref(),
            None,
        )?;
        let end_ms = apply_selector_time_modifier(
            self.grid.end_ms,
            selector.at.as_ref(),
            selector.offset.as_ref(),
            None,
        )?;
        let mut window = TimeWindow {
            start_ms: start_ms.saturating_sub(range_ms),
            end_ms,
        };
        if modifier.is_some() {
            window.start_ms = window.start_ms.saturating_sub(self.lookback_ms);
        }
        if modifier == Some(ExtendedSelectorModifier::Smoothed) {
            window.end_ms = window.end_ms.saturating_add(self.lookback_ms);
        }
        let first_end_ms = if modifier == Some(ExtendedSelectorModifier::Smoothed) {
            start_ms.saturating_add(self.lookback_ms)
        } else {
            start_ms
        };
        if start_ms != end_ms
            && self.step.millis_i64() > first_end_ms.saturating_sub(window.start_ms)
        {
            self.cacheable = false;
        }
        let matcher_sets =
            typed_matchers.map_or_else(|| label_matcher_sets(selector), <[_]>::to_vec);
        for matchers in matcher_sets {
            let key = matchers_cache_key(&matchers);
            if let Some(union) = self.unions.get_mut(&key) {
                if window.start_ms > union.end_ms || union.start_ms > window.end_ms {
                    self.cacheable = false;
                }
                union.start_ms = union.start_ms.min(window.start_ms);
                union.end_ms = union.end_ms.max(window.end_ms);
            } else {
                self.unions.insert(key, window);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn windows_follow_ranges_offsets_and_extensions() {
        for (query, expected) in [
            ("deriv(up[5m])", (3_300_000, 5_400_000)),
            ("deriv(up[1h])", (0, 5_400_000)),
            ("deriv(up[1h] offset 10m)", (-600_000, 4_800_000)),
            ("deriv(up[1h] offset -10m)", (600_000, 6_000_000)),
            ("deriv(up[1h] @ 3600)", (0, 3_600_000)),
            ("rate(up[1h] anchored)", (-300_000, 5_400_000)),
            ("rate(up[1h] smoothed)", (-300_000, 5_700_000)),
            ("up smoothed", (3_300_000, 5_700_000)),
            (
                "deriv(up[5m]) + deriv(up[1h] offset 10m)",
                (-600_000, 5_400_000),
            ),
        ] {
            let expr = crate::parse_promql(query).unwrap();
            let windows = selector_windows(&expr, 3_600_000, 5_400_000, millis(60_000), 300_000)
                .unwrap()
                .unwrap();
            assert2::assert!(windows.len() == 1, "{query}");
            let window = windows.values().next().unwrap();
            assert2::assert!((window.start_ms, window.end_ms) == expected, "{query}");
        }
    }

    #[test]
    fn subqueries_do_not_expand_other_selectors() {
        let expr = crate::parse_promql("deriv(up[1h:1m]) + deriv(other[5m])").unwrap();
        let windows = selector_windows(&expr, 3_600_000, 5_400_000, millis(60_000), 300_000)
            .unwrap()
            .unwrap();
        assert2::assert!(windows.len() == 1);
        let window = windows.values().next().unwrap();
        assert2::assert!((window.start_ms, window.end_ms) == (3_300_000, 5_400_000));
    }

    #[test]
    fn different_matcher_sets_keep_separate_windows() {
        let expr = crate::parse_promql("deriv(up[1h] offset 10m) + deriv(other[5m])").unwrap();
        let windows = selector_windows(&expr, 3_600_000, 5_400_000, millis(60_000), 300_000)
            .unwrap()
            .unwrap();
        assert2::assert!(windows.len() == 2);
        for (metric, expected) in [
            ("up", (-600_000, 4_800_000)),
            ("other", (3_300_000, 5_400_000)),
        ] {
            let matchers = [PromqlMatcher::new(
                "__name__",
                krabka_blockstore::MatchOp::Eq,
                metric,
            )];
            let window = &windows[&matchers_cache_key(&matchers)];
            assert2::assert!((window.start_ms, window.end_ms) == expected);
        }
    }

    #[test]
    fn gaps_decline_union_caching() {
        for (query, step_ms, cacheable) in [
            ("deriv(up[1h])", 86_400_000, false),
            ("deriv(up[1h])", 600_000, true),
            ("deriv(up[1h] offset 10m)", 86_400_000, false),
            ("deriv(up[1h] @ 3600)", 86_400_000, true),
            ("deriv(up[1m])", 120_000, false),
            ("deriv(up[1m])", 60_000, true),
            ("rate(up[1m] anchored)", 120_000, true),
            ("rate(up[1m] smoothed)", 120_000, true),
            (
                "deriv(up[1h] @ 3600) + deriv(up[1h] @ 90000)",
                60_000,
                false,
            ),
        ] {
            let expr = crate::parse_promql(query).unwrap();
            let windows =
                selector_windows(&expr, 3_600_000, 176_400_000, millis(step_ms), 300_000).unwrap();
            assert2::assert!(windows.is_some() == cacheable, "{query}, step={step_ms}");
        }
    }

    #[test]
    fn byte_matcher_branches_use_execution_keys() {
        let expr = crate::parse_promql(r#"deriv(up{job="\xff" or job="api"}[1h])"#).unwrap();
        let windows = selector_windows(&expr, 3_600_000, 5_400_000, millis(60_000), 300_000)
            .unwrap()
            .unwrap();
        assert2::assert!(windows.len() == 2);
        for value in [crate::PromqlString::from(vec![255]), "api".into()] {
            let matchers = vec![
                PromqlMatcher::new("__name__", krabka_blockstore::MatchOp::Eq, "up"),
                PromqlMatcher::new("job", krabka_blockstore::MatchOp::Eq, value),
            ];
            let window = &windows[&matchers_cache_key(&matchers)];
            assert2::assert!((window.start_ms, window.end_ms) == (0, 5_400_000));
        }
    }
}
