use super::*;

pub(crate) fn with_name(name: &str, labels: &[(String, String)]) -> Vec<(String, String)> {
    let mut out = Vec::with_capacity(labels.len() + 1);
    out.push(("__name__".to_string(), name.to_string()));
    out.extend(labels.iter().cloned());
    out.sort();
    out
}
