pub(crate) fn quantile_label(quantile: f64) -> String {
    let mut label = quantile.to_string();
    if label.contains('.') {
        while label.ends_with('0') {
            label.pop();
        }
        if label.ends_with('.') {
            label.push('0');
        }
    }
    label
}
