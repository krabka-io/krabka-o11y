use super::{
    CompiledLabelMatcher, CompiledLabelMatchers, LabelMatcher, MatchOp, PromqlError, Regex, Result,
};

/// Compiles a matcher set once and precompiles each `=~`/`!~` regex.
///
/// Each regex is anchored `^(?:...)$`. This function returns the same
/// regex-compile error that `labels_match` returns.
pub(crate) fn compile_label_matchers(matchers: &[LabelMatcher]) -> Result<CompiledLabelMatchers> {
    let mut compiled = Vec::with_capacity(matchers.len());
    for matcher in matchers {
        let regex = match matcher.op {
            MatchOp::Re | MatchOp::Nre => Some(
                Regex::new(&format!("^(?:{})$", matcher.value)).map_err(|error| {
                    PromqlError::Plan(format!(
                        "invalid label matcher regex for {}: {error}",
                        matcher.name
                    ))
                })?,
            ),
            MatchOp::Eq | MatchOp::Neq => None,
        };
        compiled.push(CompiledLabelMatcher {
            name: matcher.name.clone(),
            op: matcher.op,
            value: matcher.value.clone(),
            regex,
        });
    }
    Ok(CompiledLabelMatchers { matchers: compiled })
}
