use super::{Labels, expand_alert_action};

/// Expands the minimal Prometheus alert-template subset for annotation and
/// label values.
///
/// This function ignores whitespace inside the braces. It supports these
/// actions:
/// - `{{ $value }}` -> the firing sample value through [`format_sample_value`].
/// - `{{ $labels.NAME }}` / `{{ $labels."NAME" }}` -> the series label `NAME`,
///   or "" when the label is absent.
///
/// This function passes through any other `{{ ... }}` action unchanged.
/// Prometheus's `humanize` and the related functions are out of scope.
pub(crate) fn expand_alert_template(tmpl: &str, value: f64, labels: &Labels) -> String {
    let mut out = String::with_capacity(tmpl.len());
    let mut rest = tmpl;
    while let Some(open) = rest.find("{{") {
        out.push_str(&rest[..open]);
        let after_open = &rest[open + 2..];
        let Some(close) = after_open.find("}}") else {
            // No closing braces: emit the remainder verbatim.
            out.push_str(&rest[open..]);
            rest = "";
            break;
        };
        let action = after_open[..close].trim();
        let full = &rest[open..open + 2 + close + 2];
        match expand_alert_action(action, value, labels) {
            Some(expanded) => out.push_str(&expanded),
            None => out.push_str(full),
        }
        rest = &after_open[close + 2..];
    }
    out.push_str(rest);
    out
}
