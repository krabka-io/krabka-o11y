use super::*;

#[cfg(test)]
pub(crate) fn string_literal_arg(
    call: &Call,
    index: usize,
    name: &str,
) -> Result<crate::PromqlString> {
    let Some(arg) = call.args.args.get(index) else {
        return Err(PromqlError::Plan(format!(
            "{} missing {name} argument",
            call.func.name
        )));
    };
    crate::planner::byte_string_expr::string_expr_value(arg).ok_or_else(|| {
        PromqlError::Plan(format!(
            "{} {name} argument must be a string",
            call.func.name
        ))
    })
}
