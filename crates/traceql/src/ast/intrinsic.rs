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

    struct IntrinsicTagCase {
        intrinsic: Intrinsic,
        expected_name: &'static str,
    }

    /// The intrinsic tag names are Tempo's API surface: a client asking for
    /// `span:parentID` gets nothing back if this map spells it `span:parentId`.
    /// Every variant is named here, and the names are checked for being
    /// distinct as well as correct -- returning a neighbour's string is the
    /// failure a per-variant spot check misses. A new variant needs a row.
    #[test]
    fn every_intrinsic_maps_to_its_tempo_tag_name() {
        let cases = [
            IntrinsicTagCase {
                intrinsic: Intrinsic::Name,
                expected_name: "span:name",
            },
            IntrinsicTagCase {
                intrinsic: Intrinsic::Duration,
                expected_name: "span:duration",
            },
            IntrinsicTagCase {
                intrinsic: Intrinsic::Kind,
                expected_name: "span:kind",
            },
            IntrinsicTagCase {
                intrinsic: Intrinsic::Status,
                expected_name: "span:status",
            },
            IntrinsicTagCase {
                intrinsic: Intrinsic::StatusMessage,
                expected_name: "span:statusMessage",
            },
            IntrinsicTagCase {
                intrinsic: Intrinsic::Id,
                expected_name: "span:id",
            },
            IntrinsicTagCase {
                intrinsic: Intrinsic::ParentId,
                expected_name: "span:parentID",
            },
            IntrinsicTagCase {
                intrinsic: Intrinsic::ChildCount,
                expected_name: "span:childCount",
            },
            IntrinsicTagCase {
                intrinsic: Intrinsic::TraceDuration,
                expected_name: "trace:duration",
            },
            IntrinsicTagCase {
                intrinsic: Intrinsic::TraceRootName,
                expected_name: "trace:rootName",
            },
            IntrinsicTagCase {
                intrinsic: Intrinsic::TraceRootService,
                expected_name: "trace:rootService",
            },
            IntrinsicTagCase {
                intrinsic: Intrinsic::TraceId,
                expected_name: "trace:id",
            },
            IntrinsicTagCase {
                intrinsic: Intrinsic::EventName,
                expected_name: "event:name",
            },
            IntrinsicTagCase {
                intrinsic: Intrinsic::EventTimeSinceStart,
                expected_name: "event:timeSinceStart",
            },
            IntrinsicTagCase {
                intrinsic: Intrinsic::LinkTraceId,
                expected_name: "link:traceID",
            },
            IntrinsicTagCase {
                intrinsic: Intrinsic::LinkSpanId,
                expected_name: "link:spanID",
            },
            IntrinsicTagCase {
                intrinsic: Intrinsic::InstrumentationName,
                expected_name: "instrumentation:name",
            },
            IntrinsicTagCase {
                intrinsic: Intrinsic::InstrumentationVersion,
                expected_name: "instrumentation:version",
            },
            IntrinsicTagCase {
                intrinsic: Intrinsic::NestedSetLeft,
                expected_name: "span:nestedSetLeft",
            },
            IntrinsicTagCase {
                intrinsic: Intrinsic::NestedSetRight,
                expected_name: "span:nestedSetRight",
            },
            IntrinsicTagCase {
                intrinsic: Intrinsic::NestedSetParent,
                expected_name: "span:nestedSetParent",
            },
        ];

        for IntrinsicTagCase {
            intrinsic,
            expected_name,
        } in &cases
        {
            check!(intrinsic.tag_name() == *expected_name, "{intrinsic:?}");
        }

        let names = cases
            .iter()
            .map(|case| case.expected_name)
            .collect::<std::collections::BTreeSet<_>>();
        check!(
            names.len() == cases.len(),
            "every intrinsic has its own name"
        );
    }
}
