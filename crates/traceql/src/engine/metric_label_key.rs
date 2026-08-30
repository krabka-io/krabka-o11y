use super::{Field, Scope};

/// The series label key for a `by(<field>)` clause, as in real Tempo.
///
/// The key is the FULLY-SCOPED attribute name, such as
/// `resource.service.name` or `span.http.method`, not the scope-stripped key.
/// Grafana's Traces Drilldown keys its per-attribute breakdown panels on this
/// exact name. A stripped key such as `service.name` leaves the breakdown
/// blank, even though the data below it is correct. `tempo_differential`
/// verified this against real Tempo.
pub(crate) fn metric_label_key(field: &Field) -> String {
    let prefix = match &field.scope {
        Scope::Resource => "resource.",
        Scope::Span => "span.",
        Scope::Event => "event.",
        Scope::Link => "link.",
        Scope::Both => ".",
        Scope::Parent => "parent.",
        Scope::Instrumentation => "instrumentation.",
        // Intrinsics (name/status/kind/duration/trace:* …) are referenced by
        // their own names, not a scoped attribute key — keep the parser key.
        Scope::Intrinsic(_) => return field.key.clone(),
    };
    format!("{prefix}{}", field.key)
}
