use super::Regex;

pub(crate) fn decolorize_line(line: &str) -> String {
    Regex::new(r"\x1b\[[0-?]*[ -/]*[@-~]")
        .expect("ANSI CSI regex is valid")
        .replace_all(line, "")
        .into_owned()
}
