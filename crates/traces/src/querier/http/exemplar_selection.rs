use super::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ExemplarSelection {
    All,
    Limit(usize),
    None,
}

pub(crate) fn exemplar_selection(uri: &Uri) -> ExemplarSelection {
    match query_param(uri, "exemplars").as_deref() {
        Some("false" | "0") => ExemplarSelection::None,
        Some(value) => value
            .parse::<usize>()
            .map_or(ExemplarSelection::All, ExemplarSelection::Limit),
        None => ExemplarSelection::All,
    }
}
