use super::{Field, Intrinsic, Scope};

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
        Scope::Intrinsic(intrinsic) => {
            return match intrinsic {
                Intrinsic::Name => "name",
                Intrinsic::Duration => "duration",
                Intrinsic::Status => "status",
                Intrinsic::StatusMessage => "statusMessage",
                Intrinsic::Kind => "kind",
                Intrinsic::NestedSetLeft => "nestedSetLeft",
                Intrinsic::NestedSetRight => "nestedSetRight",
                Intrinsic::NestedSetParent => "nestedSetParent",
                Intrinsic::Id => "span:id",
                Intrinsic::ParentId => "span:parentID",
                Intrinsic::ChildCount => "span:childCount",
                Intrinsic::TraceId => "trace:id",
                Intrinsic::EventName => "event:name",
                Intrinsic::EventTimeSinceStart => "event:timeSinceStart",
                Intrinsic::LinkTraceId => "link:traceID",
                Intrinsic::LinkSpanId => "link:spanID",
                Intrinsic::InstrumentationName => "instrumentation:name",
                Intrinsic::InstrumentationVersion => "instrumentation:version",
                _ => &field.key,
            }
            .into();
        }
    };
    format!("{prefix}{}", field.key)
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::{Field, Intrinsic, Scope, metric_label_key};

    #[test]
    fn intrinsic_span_identity_is_canonical_and_does_not_collide_with_attribute_id() {
        assert!(
            metric_label_key(&Field {
                scope: Scope::Intrinsic(Intrinsic::Id),
                key: "id".into()
            }) == "span:id"
        );
        assert!(
            metric_label_key(&Field {
                scope: Scope::Span,
                key: "id".into()
            }) == "span.id"
        );
        assert!(
            metric_label_key(&Field {
                scope: Scope::Both,
                key: "id".into()
            }) == ".id"
        );
    }

    #[test]
    fn scoped_aliases_keep_pinned_unscoped_metric_intrinsic_names() {
        for (intrinsic, key) in [
            (Intrinsic::Name, "name"),
            (Intrinsic::Duration, "duration"),
            (Intrinsic::Status, "status"),
            (Intrinsic::StatusMessage, "statusMessage"),
            (Intrinsic::Kind, "kind"),
        ] {
            assert!(
                metric_label_key(&Field {
                    scope: Scope::Intrinsic(intrinsic),
                    key: format!("span:{key}")
                }) == key
            );
            assert!(
                metric_label_key(&Field {
                    scope: Scope::Span,
                    key: key.into()
                }) == format!("span.{key}")
            );
        }
    }
}
