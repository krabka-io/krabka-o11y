use super::*;

pub(crate) fn labels_key(labels: &Labels) -> String {
    labels.iter().fold(String::new(), |mut out, (name, value)| {
        let _ = writeln!(out, "{name}={value}");
        out
    })
}
