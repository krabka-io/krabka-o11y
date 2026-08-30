use super::{InstantSample, PromqlError, Regex, Result};

/// Applies `label_replace(v, dst_label, replacement, src_label, regex)` to an
/// already-assembled instant vector.
///
/// `regex` is fully anchored as `^(?:<regex>)$`, as in Prometheus. For each
/// series whose `src_label` value matches `regex` in full, this function sets
/// the destination label to `replacement` with `$1` and `${name}` capture-group
/// expansion. A series that does not match passes through unchanged. This
/// function keeps `__name__` unless `dst_label == "__name__"`; these functions
/// never drop the metric name themselves. An empty expansion writes
/// `dst_label=""`, because the interpreter's `Labels::insert` keeps
/// empty-valued labels. The empty entry then takes part in later collision
/// checks exactly as the interpreter sees it.
///
/// # Errors
///
/// Returns [`PromqlError::Plan`] when `regex` is not a valid regular expression.
/// The error text matches the interpreter's error text.
pub fn apply_label_replace(
    samples: Vec<InstantSample>,
    dst_label: &str,
    replacement: &str,
    src_label: &str,
    regex: &str,
) -> Result<Vec<InstantSample>> {
    // Prometheus FULLY anchors `label_replace`'s regex (`^(?:<regex>)$`), so it
    // must match the *entire* source-label value — `regexp.MatchString` on a
    // `^(?:...)$`-wrapped pattern, the same anchoring `krabka-blockstore`'s
    // `anchored_regex` applies to label matchers. A raw unanchored `Regex` would
    // wrongly match a substring (e.g. `foo` inside `xfooy`).
    let regex = Regex::new(&format!("^(?:{regex})$"))
        .map_err(|err| PromqlError::Plan(format!("invalid label_replace regex: {err}")))?;
    Ok(samples
        .into_iter()
        .map(|mut sample| {
            if let Some(captures) = regex.captures(sample.labels.get(src_label).unwrap_or("")) {
                let mut value = String::new();
                captures.expand(replacement, &mut value);
                sample.labels.insert(dst_label, value);
            }
            sample
        })
        .collect())
}
