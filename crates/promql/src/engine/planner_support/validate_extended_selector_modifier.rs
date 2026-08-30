use super::{ExtendedSelectorModifier, Result, PromqlError};

pub(crate) fn validate_extended_selector_modifier(
    function_name: &str,
    modifier: ExtendedSelectorModifier,
) -> Result<()> {
    let allowed = match modifier {
        ExtendedSelectorModifier::Anchored => matches!(
            function_name,
            "changes" | "delta" | "increase" | "rate" | "resets"
        ),
        ExtendedSelectorModifier::Smoothed => {
            matches!(function_name, "delta" | "increase" | "rate")
        }
    };
    if allowed {
        return Ok(());
    }

    let allowed_functions = match modifier {
        ExtendedSelectorModifier::Anchored => "changes, delta, increase, rate, resets",
        ExtendedSelectorModifier::Smoothed => "delta, increase, rate",
    };
    Err(PromqlError::Plan(format!(
        "{} modifier can only be used with: {allowed_functions} - not with {function_name}",
        modifier.keyword()
    )))
}
