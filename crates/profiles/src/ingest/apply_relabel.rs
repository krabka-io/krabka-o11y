use super::{Labels, RelabelAction, RelabelConfig, regex_anchored, remove_label, replace_label};

/// Apply relabel rules in order. Returns `false` when a rule rejects the series.
pub fn apply_relabel(labels: &mut Labels, configs: &[RelabelConfig]) -> bool {
    for config in configs {
        let joined = config
            .source_labels
            .iter()
            .map(|name| labels.get(name).unwrap_or(""))
            .collect::<Vec<_>>()
            .join(";");
        let Ok(regex) = regex_anchored(&config.regex) else {
            continue;
        };
        let matched = regex.is_match(&joined);

        match config.action {
            RelabelAction::Drop if matched => return false,
            RelabelAction::Keep if !matched => return false,
            RelabelAction::Replace if matched => {
                if config.replacement.is_empty() {
                    remove_label(labels, &config.target_label);
                } else {
                    replace_label(labels, &config.target_label, &config.replacement);
                }
            }
            RelabelAction::Drop | RelabelAction::Keep | RelabelAction::Replace => {}
        }
    }
    true
}
