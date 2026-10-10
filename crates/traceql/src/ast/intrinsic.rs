#[derive(Clone, Debug, PartialEq, Eq)]
/// A field supplied by the trace data model rather than an attribute map.
pub enum Intrinsic {
    Name,
    Duration,
    Kind,
    Status,
    StatusMessage,
    Id,
    ParentId,
    ChildCount,
    TraceDuration,
    TraceRootName,
    TraceRootService,
    TraceId,
    EventName,
    EventTimeSinceStart,
    LinkTraceId,
    LinkSpanId,
    InstrumentationName,
    InstrumentationVersion,
    NestedSetLeft,
    NestedSetRight,
    NestedSetParent,
}

impl Intrinsic {
    /// Tempo's canonical tag name for this intrinsic, such as `span:parentID`.
    ///
    /// The tag-name API reports these names, and planner matchers key on them.
    #[must_use]
    pub fn tag_name(&self) -> &'static str {
        match self {
            Self::Name => "span:name",
            Self::Duration => "span:duration",
            Self::Kind => "span:kind",
            Self::Status => "span:status",
            Self::StatusMessage => "span:statusMessage",
            Self::Id => "span:id",
            Self::ParentId => "span:parentID",
            Self::ChildCount => "span:childCount",
            Self::TraceDuration => "trace:duration",
            Self::TraceRootName => "trace:rootName",
            Self::TraceRootService => "trace:rootService",
            Self::TraceId => "trace:id",
            Self::EventName => "event:name",
            Self::EventTimeSinceStart => "event:timeSinceStart",
            Self::LinkTraceId => "link:traceID",
            Self::LinkSpanId => "link:spanID",
            Self::InstrumentationName => "instrumentation:name",
            Self::InstrumentationVersion => "instrumentation:version",
            Self::NestedSetLeft => "span:nestedSetLeft",
            Self::NestedSetRight => "span:nestedSetRight",
            Self::NestedSetParent => "span:nestedSetParent",
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    /// The intrinsic tag names are Tempo's API surface: a client asking for
    /// `span:parentID` gets nothing back if this map spells it `span:parentId`.
    /// Every variant is named here, and the names are checked for being
    /// distinct as well as correct -- returning a neighbour's string is the
    /// failure a per-variant spot check misses. A new variant needs a row.
    #[test]
    fn every_intrinsic_maps_to_its_tempo_tag_name() {
        let cases = [
            (Intrinsic::Name, "span:name"),
            (Intrinsic::Duration, "span:duration"),
            (Intrinsic::Kind, "span:kind"),
            (Intrinsic::Status, "span:status"),
            (Intrinsic::StatusMessage, "span:statusMessage"),
            (Intrinsic::Id, "span:id"),
            (Intrinsic::ParentId, "span:parentID"),
            (Intrinsic::ChildCount, "span:childCount"),
            (Intrinsic::TraceDuration, "trace:duration"),
            (Intrinsic::TraceRootName, "trace:rootName"),
            (Intrinsic::TraceRootService, "trace:rootService"),
            (Intrinsic::TraceId, "trace:id"),
            (Intrinsic::EventName, "event:name"),
            (Intrinsic::EventTimeSinceStart, "event:timeSinceStart"),
            (Intrinsic::LinkTraceId, "link:traceID"),
            (Intrinsic::LinkSpanId, "link:spanID"),
            (Intrinsic::InstrumentationName, "instrumentation:name"),
            (Intrinsic::InstrumentationVersion, "instrumentation:version"),
            (Intrinsic::NestedSetLeft, "span:nestedSetLeft"),
            (Intrinsic::NestedSetRight, "span:nestedSetRight"),
            (Intrinsic::NestedSetParent, "span:nestedSetParent"),
        ];

        for (intrinsic, want) in &cases {
            check!(intrinsic.tag_name() == *want, "{intrinsic:?}");
        }

        let names = cases
            .iter()
            .map(|(_, name)| *name)
            .collect::<std::collections::BTreeSet<_>>();
        check!(
            names.len() == cases.len(),
            "every intrinsic has its own name"
        );
    }
}
