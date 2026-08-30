use super::{BinModifier, Result, PromqlError};

pub(crate) fn validate_set_modifier(modifier: Option<&BinModifier>) -> Result<()> {
    let Some(modifier) = modifier else {
        return Ok(());
    };
    if modifier.fill_values.lhs.is_some() || modifier.fill_values.rhs.is_some() {
        return Err(PromqlError::Plan(
            "fill modifiers are invalid for set operators".to_string(),
        ));
    }
    Ok(())
}
