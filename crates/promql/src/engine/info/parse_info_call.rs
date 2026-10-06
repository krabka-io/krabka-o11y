use super::{
    BTreeSet, Call, Expr, InfoContext, Labels, PromqlError, Result, info_data_label_matchers,
    labels_match,
};

/// Parses and validates an `info(v [, data_label_selector])` call.
pub(crate) fn parse_info_call(call: &Call) -> Result<InfoContext<'_>> {
    let [_arg, data_label_selector @ ..] = call.args.args.as_slice() else {
        return Err(PromqlError::Plan(format!(
            "info expects one or two arguments for default target_info enrichment, got {}",
            call.args.args.len()
        )));
    };
    if data_label_selector.len() > 1 {
        return Err(PromqlError::Plan(format!(
            "info expects one or two arguments for default target_info enrichment, got {}",
            call.args.args.len()
        )));
    }
    let mut byte_matchers = None;
    let data_label_selector = match data_label_selector {
        [] => None,
        [selector] => match selector.as_ref() {
            Expr::VectorSelector(selector) => Some(selector),
            Expr::Extension(extension) => {
                let Some(byte) = extension
                    .expr
                    .as_any()
                    .downcast_ref::<crate::planner::byte_selector_expr::ByteSelectorExpr>(
                ) else {
                    return Err(PromqlError::Plan(
                        "info data label selector must be a vector selector".to_owned(),
                    ));
                };
                let Expr::VectorSelector(selector) = &byte.child else {
                    return Err(PromqlError::Plan(
                        "info data label selector must be a vector selector".to_owned(),
                    ));
                };
                let [matchers] = byte.matcher_sets.as_slice() else {
                    return Err(PromqlError::Plan(
                        "info data label selector does not support or matchers".to_owned(),
                    ));
                };
                byte_matchers = Some(matchers.clone());
                Some(selector)
            }
            _ => {
                return Err(PromqlError::Plan(
                    "info data label selector must be a vector selector".to_string(),
                ));
            }
        },
        [_, _, ..] => unreachable!("data label selector arity checked above"),
    };
    let data_label_matchers = if let Some(matchers) = byte_matchers {
        matchers
    } else {
        data_label_selector
            .map(info_data_label_matchers)
            .transpose()?
            .unwrap_or_default()
    };
    let required_data_label_matchers = data_label_matchers
        .iter()
        .filter(|matcher| matcher.name != "__name__")
        .cloned()
        .collect::<Vec<_>>();
    let required_data_label_matchers_match_empty =
        labels_match(&Labels::new(), &required_data_label_matchers)?;
    let selected_data_labels = data_label_matchers
        .iter()
        .filter(|matcher| matcher.name != "__name__")
        .map(|matcher| matcher.name.clone())
        .collect::<BTreeSet<_>>();
    let restrict_data_labels = data_label_selector.is_some() && !selected_data_labels.is_empty();
    Ok(InfoContext {
        data_label_selector,
        data_label_matchers,
        required_data_label_matchers_match_empty,
        selected_data_labels,
        restrict_data_labels,
    })
}
