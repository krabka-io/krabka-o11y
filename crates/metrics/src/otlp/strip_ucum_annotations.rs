use super::*;

pub(crate) fn strip_ucum_annotations(unit: &str) -> String {
    let mut out = String::with_capacity(unit.len());
    let mut in_annotation = false;
    for ch in unit.chars() {
        match ch {
            '{' => in_annotation = true,
            '}' if in_annotation => in_annotation = false,
            _ if !in_annotation => out.push(ch),
            _ => {}
        }
    }
    out
}
