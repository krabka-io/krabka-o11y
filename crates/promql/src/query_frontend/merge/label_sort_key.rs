use super::*;

pub(crate) fn label_sort_key(labels: &Labels) -> String {
    labels.iter().fold(String::new(), |mut out, pair| {
        let _ = writeln!(out, "{}={}", pair.name, pair.value);
        out
    })
}
