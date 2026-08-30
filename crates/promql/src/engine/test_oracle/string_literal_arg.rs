use super::*;

#[cfg(test)]
pub(crate) fn string_literal_arg(call: &Call, index: usize, name: &str) -> Result<String> {
    let Some(arg) = call.args.args.get(index) else {
        return Err(PromqlError::Plan(format!(
            "{} missing {name} argument",
            call.func.name
        )));
    };
    let Expr::StringLiteral(value) = arg.as_ref() else {
        return Err(PromqlError::Plan(format!(
            "{} {name} argument must be a string",
            call.func.name
        )));
    };
    Ok(value.val.clone())
}
