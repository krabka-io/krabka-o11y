//! `SpanStore` implementation over cold span blocks plus the live tier.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use arc_swap::ArcSwap;
use arrow::{
    array::{
        Array, ArrayRef, BooleanArray, DictionaryArray, FixedSizeBinaryArray,
        FixedSizeBinaryBuilder, Float64Array, Int32Array, Int64Array, Int64Builder,
        LargeStringArray, ListArray, StringArray, StringBuilder, StringViewArray, StructArray,
        UInt32Array,
    },
    compute::{cast, concat_batches, filter_record_batch, take},
    datatypes::{DataType, Field, Int32Type, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use datafusion::{catalog::MemTable, prelude::SessionContext};
use krabka_blockstore::{
    BlockIndex, BlockStore, SCOL_ATTR_KEYS, SCOL_ATTR_VALUE, SCOL_ATTR_VALUE_BOOL,
    SCOL_ATTR_VALUE_DOUBLE, SCOL_ATTR_VALUE_INT, SCOL_EVENTS, SCOL_LINKS, TraceIndex,
    span_block_schema,
};
use krabka_traceql::{
    ATTR_PREFIX, AttrValue, COL_CHILD_COUNT, COL_DURATION, COL_EVENT_NAME,
    COL_EVENT_TIME_SINCE_START, COL_INSTRUMENTATION_NAME, COL_INSTRUMENTATION_VERSION, COL_KIND,
    COL_LINK_SPAN_ID, COL_LINK_TRACE_ID, COL_NAME, COL_NS_LEFT, COL_NS_RIGHT, COL_PARENT_ID,
    COL_PARENT_SPAN_ID, COL_ROOT_SERVICE_NAME, COL_ROOT_SPAN_NAME, COL_SPAN_ID, COL_START,
    COL_STATUS_CODE, COL_STATUS_MESSAGE, COL_TRACE_DURATION, COL_TRACE_ID, EVENT_ATTR_PREFIX,
    EventRef, INSTRUMENTATION_ATTR_PREFIX, LINK_ATTR_PREFIX, LinkRef, MatchCmp, MatchScope,
    MatchValue, ScanJob, ScanOptions, ScanResult, ScopedTag, SpanMatcher, SpanRef, SpanStore,
    TagScope, TraceSpans, TraceqlError, TypedValue, span_schema,
};
use krabka_units::{
    ByteSize, Time,
    convert::{ByteSizeExt as _, TimeExt},
};

use crate::{querier::live::LiveTier, span::batch::RESOURCE_ATTR_PREFIX};

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    fn matcher(scope: MatchScope, key: &str, op: MatchCmp, value: MatchValue) -> SpanMatcher {
        SpanMatcher {
            scope,
            key: key.to_string(),
            op,
            value,
            negated: false,
        }
    }

    /// `collect_intrinsic_value` reports a tag's value along with the type
    /// name a client should read it as, so both halves are pinned. The type is
    /// the easier half to get wrong: it is a literal beside the value rather
    /// than derived from the column, so a duration labelled "int" or an id
    /// labelled "duration" is a change nothing about the value can reveal.
    #[test]
    fn collecting_an_intrinsic_reports_its_value_and_type() {
        let batch = span_batch(&[span_with_nested_refs()]).expect("one-span batch");
        let collect = |tag: &str| {
            let mut values = BTreeSet::new();
            super::collect_intrinsic_value(&batch, 0, tag, &mut values).expect("readable");
            values.into_iter().collect::<Vec<_>>()
        };
        let pair = |type_: &str, value: &str| (type_.to_string(), value.to_string());

        // Durations carry their own type name rather than "int".
        check!(collect("span:duration") == vec![pair("duration", "500")]);
        check!(collect("trace:duration") == vec![pair("duration", "500")]);

        // Counts and enumerations are ints.
        check!(
            collect("span:kind") == vec![pair("int", "2")],
            "server is kind 2"
        );
        check!(
            collect("span:status") == vec![pair("int", "1")],
            "ok is status 1"
        );
        check!(collect("span:childCount") == vec![pair("int", "0")]);
        check!(collect("span:nestedSetLeft") == vec![pair("int", "1")]);
        check!(collect("span:nestedSetRight") == vec![pair("int", "2")]);
        check!(
            collect("span:Parent") == vec![pair("int", "-1")],
            "a root has no parent index"
        );

        // Ids render as hex strings.
        check!(collect("span:id") == vec![pair("string", "0202020202020202")]);
        check!(collect("trace:id") == vec![pair("string", "01010101010101010101010101010101")]);

        // Text columns.
        check!(collect("span:name") == vec![pair("string", "GET /users")]);
        check!(collect("trace:rootName") == vec![pair("string", "GET /users")]);
        check!(collect("trace:rootService") == vec![pair("string", "api")]);
        check!(collect("instrumentation:name") == vec![pair("string", "otel-rust")]);
        check!(collect("instrumentation:version") == vec![pair("string", "1.2.3")]);

        // A null parent id contributes nothing rather than an empty string,
        // which would otherwise appear as a real tag value in the results.
        check!(
            collect("span:parentID") == vec![],
            "this span has no parent"
        );

        // An empty status message is skipped, and a real one is not. Both
        // sides are needed: with only the empty case, dropping the emptiness
        // check changes nothing observable.
        check!(collect("span:statusMessage") == vec![], "empty is omitted");
        let mut failed = span_with_nested_refs();
        failed.status_message = "upstream timeout".into();
        let failed_batch = span_batch(&[failed]).expect("one-span batch");
        let mut message = BTreeSet::new();
        super::collect_intrinsic_value(&failed_batch, 0, "span:statusMessage", &mut message)
            .expect("readable");
        check!(
            message.into_iter().collect::<Vec<_>>() == vec![pair("string", "upstream timeout")],
            "a real message is reported"
        );

        // An unknown tag collects nothing and is not an error.
        check!(collect("span:nonsense") == vec![]);
        check!(collect("") == vec![]);
    }

    /// `intrinsic_matches` reads a different column per key, so the fixture is
    /// a real span batch rather than a hand-built one: a batch missing a
    /// column would fail to resolve rather than report a mismatch, and the
    /// nested event and link columns only exist on a span that has them.
    ///
    /// The span here is `span_with_nested_refs`, which carries one event, one
    /// link, resource and span attributes, and an instrumentation scope.
    #[test]
    fn every_span_intrinsic_reads_its_own_column() {
        // A second event and link, so that "one of them matches" is a
        // different question from "all of them do". With a single event the
        // two agree and neither can be tested.
        let mut span = span_with_nested_refs();
        span.events.push(EventRecord {
            time_unix_nano: 1_200,
            name: "retry".into(),
            attrs: Vec::new(),
        });
        span.links.push(LinkRecord {
            trace_id: [7; 16],
            span_id: [6; 8],
            attrs: Vec::new(),
        });
        let batch = span_batch(&[span]).expect("one-span batch");
        let hit = |key: &str, value: MatchValue| {
            super::intrinsic_matches(
                &batch,
                0,
                &matcher(MatchScope::Intrinsic, key, MatchCmp::Eq, value),
            )
            .expect("intrinsic is readable")
        };
        let str_hit = |key: &str, value: &str| hit(key, MatchValue::Str(value.to_string()));

        // Flat span columns.
        check!(str_hit("span:name", "GET /users"));
        check!(
            !str_hit("span:name", "GET /orders"),
            "a different name does not match"
        );
        check!(hit("span:duration", MatchValue::Int(500)));
        check!(!hit("span:duration", MatchValue::Int(501)));
        check!(str_hit("span:id", "0202020202020202"), "ids render as hex");
        check!(str_hit("trace:id", "01010101010101010101010101010101"));
        check!(str_hit("span:statusMessage", ""));

        // The instrumentation scope is its own pair of columns.
        check!(str_hit("instrumentation:name", "otel-rust"));
        check!(str_hit("instrumentation:version", "1.2.3"));
        check!(
            !str_hit("instrumentation:name", "1.2.3"),
            "the two are not interchangeable"
        );

        // Trace-level columns are derived from the span set, not the span.
        check!(str_hit("trace:rootService", "api"));
        check!(str_hit("trace:rootName", "GET /users"));
        check!(hit("trace:duration", MatchValue::Int(500)));

        // Nested columns: the event and link the span actually carries.
        check!(
            str_hit("event:name", "exception"),
            "the first event matches"
        );
        check!(str_hit("event:name", "retry"), "and so does the second");
        check!(
            !str_hit("event:name", "timeout"),
            "the attribute is not the name"
        );
        check!(
            hit("event:timeSinceStart", MatchValue::Int(50)),
            "relative to span start"
        );
        check!(
            hit("event:timeSinceStart", MatchValue::Int(200)),
            "the second event too"
        );
        check!(str_hit("link:traceID", "09090909090909090909090909090909"));
        check!(str_hit("link:spanID", "0808080808080808"));
        check!(
            str_hit("link:traceID", "07070707070707070707070707070707"),
            "the second link"
        );
        check!(
            !str_hit("link:traceID", "0808080808080808"),
            "the link's two ids are not interchangeable"
        );

        // An unknown intrinsic matches everything rather than nothing. That is
        // the permissive default a filter wants -- an unrecognised predicate
        // excludes no rows instead of silently emptying the result -- but it
        // is the opposite of what the named keys above do, so it is pinned.
        check!(str_hit("span:nonsense", "anything"));
        check!(str_hit("", "anything"), "including an empty key");
    }

    /// `link_matcher_matches_link` answers one matcher against one link. Its
    /// two scopes read different halves of the link -- Link reads the
    /// attributes, Intrinsic reads the two ids -- so a hit and a miss are
    /// pinned in each: a mutant that answers a constant is invisible to
    /// whichever half it happens to agree with.
    #[test]
    fn a_link_matcher_reads_the_link_it_is_given() {
        let link = LinkRef {
            trace_id: [9; 16],
            span_id: [8; 8],
            // Two keys with different values, so a mutant that inverts the
            // key filter selects the *other* attribute rather than nothing,
            // and returns a wrong answer instead of an empty one.
            attributes: vec![
                ("link.kind".into(), AttrValue::Str("retry".into())),
                ("link.reason".into(), AttrValue::Str("timeout".into())),
            ],
        };
        let m = |scope, key, op, value| {
            super::link_matcher_matches_link(&link, &matcher(scope, key, op, value))
        };
        let eq =
            |scope, key, value: &str| m(scope, key, MatchCmp::Eq, MatchValue::Str(value.into()));

        // Link scope reads the attributes, and each key reads its own value.
        check!(eq(MatchScope::Link, "link.kind", "retry"));
        check!(eq(MatchScope::Link, "link.reason", "timeout"));
        check!(
            !eq(MatchScope::Link, "link.kind", "timeout"),
            "not the other key's value"
        );
        check!(!eq(MatchScope::Link, "link.reason", "retry"));
        check!(
            !eq(MatchScope::Link, "link.absent", "retry"),
            "an absent key matches nothing"
        );

        // Intrinsic scope reads the two ids, hex-encoded and not interchangeable.
        let trace_hex = "09090909090909090909090909090909";
        let span_hex = "0808080808080808";
        check!(eq(MatchScope::Intrinsic, "link:traceID", trace_hex));
        check!(eq(MatchScope::Intrinsic, "link:spanID", span_hex));
        check!(
            !eq(MatchScope::Intrinsic, "link:traceID", span_hex),
            "ids do not cross over"
        );
        check!(!eq(MatchScope::Intrinsic, "link:spanID", trace_hex));

        // A link always has both ids, so `= nil` is false and `!= nil` holds.
        check!(!m(
            MatchScope::Intrinsic,
            "link:traceID",
            MatchCmp::Eq,
            MatchValue::Nil
        ));
        check!(m(
            MatchScope::Intrinsic,
            "link:traceID",
            MatchCmp::Neq,
            MatchValue::Nil
        ));
        check!(!m(
            MatchScope::Intrinsic,
            "link:spanID",
            MatchCmp::Eq,
            MatchValue::Nil
        ));
        check!(m(
            MatchScope::Intrinsic,
            "link:spanID",
            MatchCmp::Neq,
            MatchValue::Nil
        ));

        // An unrecognised intrinsic key is a non-match here. Note this is the
        // opposite of the span-level matcher, which lets an unknown intrinsic
        // through; the two defaults disagree and both are load-bearing.
        check!(!eq(MatchScope::Intrinsic, "link:nonsense", "anything"));
        check!(!eq(MatchScope::Intrinsic, "", "anything"));

        // Any other scope is a non-match whatever the key says.
        check!(!eq(MatchScope::Span, "link.kind", "retry"));
        check!(!eq(MatchScope::Resource, "link.kind", "retry"));
        check!(!eq(MatchScope::Event, "link.kind", "retry"));

        // Negation inverts whatever the answer was, in both directions.
        let negated = |scope, key, value: &str| {
            let mut matcher = matcher(scope, key, MatchCmp::Eq, MatchValue::Str(value.into()));
            matcher.negated = true;
            super::link_matcher_matches_link(&link, &matcher)
        };
        check!(
            !negated(MatchScope::Link, "link.kind", "retry"),
            "a hit becomes a miss"
        );
        check!(
            negated(MatchScope::Link, "link.kind", "timeout"),
            "and a miss becomes a hit"
        );
    }

    /// A span with no links still answers link matchers: `= nil` holds and
    /// `!= nil` does not. Everything else is a non-match, and a negated
    /// matcher inverts whatever the answer was.
    #[test]
    fn a_span_without_links_matches_only_absence() {
        let m = |scope, key, op, value| {
            super::link_matcher_matches_absence(&matcher(scope, key, op, value))
        };

        // Scoped at the link itself.
        check!(m(
            MatchScope::Link,
            "anything",
            MatchCmp::Eq,
            MatchValue::Nil
        ));
        check!(!m(
            MatchScope::Link,
            "anything",
            MatchCmp::Neq,
            MatchValue::Nil
        ));
        check!(
            !m(
                MatchScope::Link,
                "anything",
                MatchCmp::Eq,
                MatchValue::Int(1)
            ),
            "a real value cannot match a link that is not there"
        );

        // The two link intrinsics behave the same way.
        for key in ["link:traceID", "link:spanID"] {
            check!(
                m(MatchScope::Intrinsic, key, MatchCmp::Eq, MatchValue::Nil),
                "{key}"
            );
            check!(
                !m(MatchScope::Intrinsic, key, MatchCmp::Neq, MatchValue::Nil),
                "{key}"
            );
        }

        // An intrinsic that is not about links does not match at all.
        check!(!m(
            MatchScope::Intrinsic,
            "span:name",
            MatchCmp::Eq,
            MatchValue::Nil
        ));
        check!(!m(
            MatchScope::Intrinsic,
            "event:name",
            MatchCmp::Eq,
            MatchValue::Nil
        ));

        // Nor does any other scope.
        for scope in [MatchScope::Span, MatchScope::Resource, MatchScope::Event] {
            check!(!m(scope, "anything", MatchCmp::Eq, MatchValue::Nil));
        }

        // Negation flips the answer, both ways.
        let mut negated = matcher(MatchScope::Link, "x", MatchCmp::Eq, MatchValue::Nil);
        negated.negated = true;
        check!(
            !super::link_matcher_matches_absence(&negated),
            "a negated absence match is a non-match"
        );
        negated.op = MatchCmp::Neq;
        check!(
            super::link_matcher_matches_absence(&negated),
            "and a negated non-match is a match"
        );
    }

    /// Events mirror links exactly, over their own scope and intrinsics.
    #[test]
    fn a_span_without_events_matches_only_absence() {
        let m = |scope, key, op, value| {
            super::event_matcher_matches_absence(&matcher(scope, key, op, value))
        };

        check!(m(
            MatchScope::Event,
            "anything",
            MatchCmp::Eq,
            MatchValue::Nil
        ));
        check!(!m(
            MatchScope::Event,
            "anything",
            MatchCmp::Neq,
            MatchValue::Nil
        ));

        for key in ["event:name", "event:timeSinceStart"] {
            check!(
                m(MatchScope::Intrinsic, key, MatchCmp::Eq, MatchValue::Nil),
                "{key}"
            );
            check!(
                !m(MatchScope::Intrinsic, key, MatchCmp::Neq, MatchValue::Nil),
                "{key}"
            );
        }

        // A link intrinsic is not an event intrinsic, and the reverse holds
        // in the link matcher above -- the two must not answer for each other.
        check!(!m(
            MatchScope::Intrinsic,
            "link:traceID",
            MatchCmp::Eq,
            MatchValue::Nil
        ));
        check!(!m(
            MatchScope::Link,
            "anything",
            MatchCmp::Eq,
            MatchValue::Nil
        ));

        let mut negated = matcher(MatchScope::Event, "x", MatchCmp::Eq, MatchValue::Nil);
        negated.negated = true;
        check!(!super::event_matcher_matches_absence(&negated));
    }

    /// `nil_matches` and `nested_presence_matches` are the two ways a matcher
    /// asks about presence. The first says whether a value that exists can
    /// match; the second answers for a whole collection and declines to
    /// answer for any operator other than equality.
    #[test]
    fn presence_matchers_answer_only_about_nil() {
        check!(super::nil_matches(MatchCmp::Eq, &MatchValue::Nil));
        check!(!super::nil_matches(MatchCmp::Neq, &MatchValue::Nil));
        check!(!super::nil_matches(MatchCmp::Eq, &MatchValue::Int(0)));
        check!(!super::nil_matches(MatchCmp::Lt, &MatchValue::Nil));

        // A value that is present is not nil, and differs from nil.
        check!(super::present_value_matches(MatchCmp::Eq, &MatchValue::Nil) == Some(false));
        check!(super::present_value_matches(MatchCmp::Neq, &MatchValue::Nil) == Some(true));
        check!(
            super::present_value_matches(MatchCmp::Eq, &MatchValue::Int(1)) == None,
            "a real comparison is left to the caller"
        );
        check!(super::present_value_matches(MatchCmp::Lt, &MatchValue::Nil) == None);

        // A collection answers about itself, so the sense flips with content.
        check!(super::nested_presence_matches(false, MatchCmp::Eq, &MatchValue::Nil) == Some(true));
        check!(super::nested_presence_matches(true, MatchCmp::Eq, &MatchValue::Nil) == Some(false));
        check!(
            super::nested_presence_matches(false, MatchCmp::Neq, &MatchValue::Nil) == Some(false)
        );
        check!(super::nested_presence_matches(true, MatchCmp::Neq, &MatchValue::Nil) == Some(true));
        check!(
            super::nested_presence_matches(true, MatchCmp::Eq, &MatchValue::Int(1)) == None,
            "only nil is answered here"
        );
    }

    /// The comparison operators are the whole of a matcher's meaning, so each
    /// is checked on the boundary where the strict and non-strict forms part
    /// company, and either side of it so a comparison stuck on one answer is
    /// caught too.
    #[test]
    fn integer_comparisons_are_exact_at_the_boundary() {
        let five = MatchValue::Int(5);
        let cmp = |value, op| super::int_matches(value, op, &five);

        check!(cmp(5, MatchCmp::Eq));
        check!(!cmp(5, MatchCmp::Neq));
        check!(!cmp(5, MatchCmp::Lt), "5 < 5");
        check!(cmp(5, MatchCmp::Lte), "5 <= 5");
        check!(!cmp(5, MatchCmp::Gt), "5 > 5");
        check!(cmp(5, MatchCmp::Gte), "5 >= 5");

        check!(cmp(4, MatchCmp::Lt) && cmp(4, MatchCmp::Lte) && cmp(4, MatchCmp::Neq));
        check!(!cmp(4, MatchCmp::Gt) && !cmp(4, MatchCmp::Gte) && !cmp(4, MatchCmp::Eq));
        check!(cmp(6, MatchCmp::Gt) && cmp(6, MatchCmp::Gte) && cmp(6, MatchCmp::Neq));
        check!(!cmp(6, MatchCmp::Lt) && !cmp(6, MatchCmp::Lte) && !cmp(6, MatchCmp::Eq));

        // Regex operators have no meaning against a number.
        check!(!cmp(5, MatchCmp::Re) && !cmp(5, MatchCmp::Nre));

        // A value of another type never matches, whatever the operator.
        for op in [MatchCmp::Eq, MatchCmp::Lt, MatchCmp::Gte] {
            check!(
                !super::int_matches(5, op, &MatchValue::Bool(true)),
                "an integer does not compare with a bool"
            );
        }
    }

    /// The float matcher mirrors the integer one, with the extra case that
    /// NaN compares equal to nothing at all -- not even itself -- which is
    /// what the partial comparison is there to express.
    #[test]
    fn float_comparisons_are_exact_and_nan_matches_nothing() {
        let five = MatchValue::Float(5.0);
        let cmp = |value, op| super::float_matches(value, op, &five);

        check!(cmp(5.0, MatchCmp::Eq));
        check!(!cmp(5.0, MatchCmp::Lt) && cmp(5.0, MatchCmp::Lte));
        check!(!cmp(5.0, MatchCmp::Gt) && cmp(5.0, MatchCmp::Gte));
        check!(cmp(4.5, MatchCmp::Lt) && cmp(5.5, MatchCmp::Gt));

        // NaN is unordered against everything.
        check!(!cmp(f64::NAN, MatchCmp::Eq), "NaN is equal to nothing");
        check!(
            cmp(f64::NAN, MatchCmp::Neq),
            "so it differs from everything"
        );
        check!(!cmp(f64::NAN, MatchCmp::Lt) && !cmp(f64::NAN, MatchCmp::Gt));
        check!(!cmp(f64::NAN, MatchCmp::Lte) && !cmp(f64::NAN, MatchCmp::Gte));

        check!(
            !super::float_matches(5.0, MatchCmp::Eq, &MatchValue::Int(5)),
            "a float does not compare with an integer"
        );
    }

    /// Booleans support only equality; every ordering operator is a
    /// non-match rather than an error.
    #[test]
    fn booleans_compare_only_for_equality() {
        let yes = MatchValue::Bool(true);

        check!(super::bool_matches(true, MatchCmp::Eq, &yes));
        check!(!super::bool_matches(false, MatchCmp::Eq, &yes));
        check!(super::bool_matches(false, MatchCmp::Neq, &yes));
        check!(!super::bool_matches(true, MatchCmp::Neq, &yes));

        for op in [
            MatchCmp::Lt,
            MatchCmp::Lte,
            MatchCmp::Gt,
            MatchCmp::Gte,
            MatchCmp::Re,
            MatchCmp::Nre,
        ] {
            check!(
                !super::bool_matches(true, op, &yes),
                "ordering has no meaning"
            );
        }

        check!(
            !super::bool_matches(true, MatchCmp::Eq, &MatchValue::Int(1)),
            "a bool does not compare with an integer"
        );
    }

    /// Span kind and status names come off the wire as strings and have to
    /// land on the numbers the stored spans use. Every name is checked, since
    /// a table is exactly where an off-by-one goes unnoticed.
    #[test]
    fn span_kind_and_status_names_map_to_their_stored_numbers() {
        let kinds = [
            ("unspecified", 0),
            ("internal", 1),
            ("server", 2),
            ("client", 3),
            ("producer", 4),
            ("consumer", 5),
        ];
        for (name, value) in kinds {
            check!(super::kind_enum_value(name) == Some(value), "kind {name}");
        }
        check!(
            super::kind_enum_value("Server") == None,
            "the match is case-sensitive"
        );
        check!(super::kind_enum_value("") == None);
        check!(
            super::kind_enum_value("gateway") == None,
            "an unknown kind is not a number"
        );

        for (name, value) in [("unset", 0), ("ok", 1), ("error", 2)] {
            check!(
                super::status_enum_value(name) == Some(value),
                "status {name}"
            );
        }
        check!(
            super::status_enum_value("OK") == None,
            "the match is case-sensitive"
        );
        check!(super::status_enum_value("failed") == None);
    }

    /// Trace and span ids are rendered as lower-case hex, two characters per
    /// byte, with leading zeroes kept -- a byte dropping its high nibble
    /// would produce an id that no longer round-trips.
    #[test]
    fn bytes_render_as_two_hex_characters_each() {
        let hex = super::bytes_to_hex;

        check!(hex(&[]) == "");
        check!(
            hex(&[0x00]) == "00",
            "a zero byte is two characters, not none"
        );
        check!(hex(&[0x0f]) == "0f", "the leading zero is kept");
        check!(hex(&[0xf0]) == "f0");
        check!(hex(&[0xff]) == "ff");
        check!(hex(&[0xab, 0xcd]) == "abcd", "bytes keep their order");
        check!(hex(&[0x01, 0x23, 0x45, 0x67]) == "01234567");
        check!(
            hex(&[0x89, 0xab, 0xcd, 0xef]) == "89abcdef",
            "digits above nine are lower case"
        );
        check!(hex(&[0xde; 16]).len() == 32, "a trace id is 32 characters");
    }

    use arc_swap::ArcSwap;
    use arrow::{
        array::{
            ArrayRef, BooleanArray, FixedSizeBinaryBuilder, Float64Array, Int32Array, Int64Array,
            ListArray, ListBuilder, PrimitiveDictionaryBuilder, StringArray, StringBuilder,
            StringDictionaryBuilder,
        },
        buffer::{NullBuffer, OffsetBuffer},
        datatypes::{DataType, Field, Int32Type, Int64Type, Schema, SchemaRef},
    };
    use assert2::check;
    use krabka_blockstore::{
        AttrValue as BlockAttrValue, BlockWriter, NestedSet as BlockNestedSet, PromotedSpanAttr,
        SCOL_START_NANO, SCOL_TRACE_ID, ShardedTraceBloom, SpanAttr, SpanKind as BlockSpanKind,
        SpanRow, StatusCode as BlockStatusCode, SummaryColumns, TraceBlockStats, encode_span_rows,
        span_block_decl, span_block_schema,
    };
    use krabka_traceql::{
        COL_CHILD_COUNT, COL_INSTRUMENTATION_NAME, COL_INSTRUMENTATION_VERSION, EngineOpts,
        EventRef, LinkRef, ScanJob, ScanOptions, TraceqlEngine,
    };
    use krabka_units::{convert::ByteSizeExt as _, nanos};
    use object_store::{buffered::BufWriter, memory::InMemory, path::Path};
    use parquet::{arrow::AsyncArrowWriter, file::properties::WriterProperties};
    use url::Url;

    use super::*;
    use crate::{
        querier::live::LiveSource,
        span::{
            AttrValue as SpanAttrValue, EventRecord, KeyValue, LinkRecord, Span, SpanKind,
            StatusCode,
            batch::{span_batch, span_batch_with_promoted_attrs},
        },
    };

    fn shared(index: TraceIndex) -> SharedTraceIndex {
        Arc::new(ArcSwap::from_pointee(index))
    }

    #[test]
    fn integer_matchers_distinguish_equal_and_unequal_values() {
        let expected = MatchValue::Int(7);
        assert2::assert!(int_matches(7, MatchCmp::Eq, &expected));
        assert2::assert!(!int_matches(8, MatchCmp::Eq, &expected));
        assert2::assert!(!int_matches(7, MatchCmp::Neq, &expected));
        assert2::assert!(int_matches(8, MatchCmp::Neq, &expected));
    }

    #[test]
    fn float_matchers_distinguish_equal_and_unequal_values() {
        let expected = MatchValue::Float(7.5);
        assert2::assert!(float_matches(7.5, MatchCmp::Eq, &expected));
        assert2::assert!(!float_matches(8.5, MatchCmp::Eq, &expected));
        assert2::assert!(!float_matches(7.5, MatchCmp::Neq, &expected));
        assert2::assert!(float_matches(8.5, MatchCmp::Neq, &expected));
    }

    #[derive(Default)]
    struct FakeLiveSource {
        trace: Option<TraceSpans>,
        batches: Vec<RecordBatch>,
        values: Vec<TypedValue>,
        frontier_ns: i64,
    }

    #[async_trait::async_trait]
    impl LiveSource for FakeLiveSource {
        async fn span_batches(
            &self,
            _tenant: &str,
            _start_ns: i64,
            _end_ns: i64,
        ) -> Result<Vec<RecordBatch>, TraceqlError> {
            Ok(self.batches.clone())
        }

        async fn trace_spans(
            &self,
            _tenant: &str,
            _trace_id: &[u8; 16],
        ) -> Result<Option<TraceSpans>, TraceqlError> {
            Ok(self.trace.clone())
        }

        async fn tag_names(
            &self,
            _tenant: &str,
            _scope: Option<TagScope>,
            _start_ns: i64,
            _end_ns: i64,
        ) -> Result<Vec<ScopedTag>, TraceqlError> {
            Ok(Vec::new())
        }

        async fn tag_values(
            &self,
            _tenant: &str,
            _tag: &str,
            _start_ns: i64,
            _end_ns: i64,
        ) -> Result<Vec<TypedValue>, TraceqlError> {
            Ok(self.values.clone())
        }

        fn block_builder_frontier_ns(&self, _tenant: &str) -> i64 {
            self.frontier_ns
        }
    }

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new(COL_TRACE_ID, DataType::FixedSizeBinary(16), false),
            Field::new(COL_SPAN_ID, DataType::FixedSizeBinary(8), false),
            Field::new(COL_PARENT_SPAN_ID, DataType::FixedSizeBinary(8), true),
            Field::new("nested_set_left", DataType::Int32, false),
            Field::new("nested_set_right", DataType::Int32, false),
            Field::new("parent_id", DataType::Int32, false),
            Field::new(COL_CHILD_COUNT, DataType::Int32, false),
            Field::new(COL_ROOT_SERVICE_NAME, DataType::Utf8, true),
            Field::new(COL_ROOT_SPAN_NAME, DataType::Utf8, true),
            Field::new("trace_start_unix_nano", DataType::Int64, false),
            Field::new("trace_duration_nanos", DataType::Int64, false),
            Field::new(COL_NAME, DataType::Utf8, true),
            Field::new("kind", DataType::Int32, false),
            Field::new(COL_START, DataType::Int64, false),
            Field::new(COL_DURATION, DataType::Int64, false),
            Field::new("status_code", DataType::Int32, false),
            Field::new("status_message", DataType::Utf8, true),
            Field::new(COL_INSTRUMENTATION_NAME, DataType::Utf8, true),
            Field::new(COL_INSTRUMENTATION_VERSION, DataType::Utf8, true),
            Field::new(format!("{ATTR_PREFIX}svc"), DataType::Utf8, true),
        ]))
    }

    fn batch() -> RecordBatch {
        let schema = test_schema();
        let mut trace_id = FixedSizeBinaryBuilder::with_capacity(2, 16);
        trace_id.append_value([7; 16]).unwrap();
        trace_id.append_value([9; 16]).unwrap();
        let mut span_id = FixedSizeBinaryBuilder::with_capacity(2, 8);
        span_id.append_value([1; 8]).unwrap();
        span_id.append_value([2; 8]).unwrap();
        let mut parent_id = FixedSizeBinaryBuilder::with_capacity(2, 8);
        parent_id.append_null();
        parent_id.append_null();

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(trace_id.finish()) as ArrayRef,
                Arc::new(span_id.finish()),
                Arc::new(parent_id.finish()),
                Arc::new(Int32Array::from(vec![1, 1])),
                Arc::new(Int32Array::from(vec![2, 2])),
                Arc::new(Int32Array::from(vec![0, 0])),
                Arc::new(Int32Array::from(vec![0, 0])),
                Arc::new(StringArray::from(vec!["api", "web"])),
                Arc::new(StringArray::from(vec!["GET /", "GET /x"])),
                Arc::new(Int64Array::from(vec![100, 200])),
                Arc::new(Int64Array::from(vec![10, 20])),
                Arc::new(StringArray::from(vec!["root", "other"])),
                Arc::new(Int32Array::from(vec![0, 0])),
                Arc::new(Int64Array::from(vec![100, 200])),
                Arc::new(Int64Array::from(vec![10, 20])),
                Arc::new(Int32Array::from(vec![0, 0])),
                Arc::new(StringArray::from(vec!["", ""])),
                Arc::new(StringArray::from(vec!["tracer", "tracer"])),
                Arc::new(StringArray::from(vec!["", ""])),
                Arc::new(StringArray::from(vec!["a", "b"])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn scan_concat_max_is_dimensioned() {
        assert2::assert!(DEFAULT_SCAN_CONCAT_MAX == krabka_units::bytes(1_500_000_000));
    }

    #[test]
    fn scan_concat_cap_accepts_exact_size_and_rejects_one_byte_less() {
        let batch = batch();
        let size = u64::try_from(batch.get_array_memory_size()).unwrap();
        let exact = ByteSize::from_bytes(size);
        assert2::assert!(recompute_scan_nested_sets(vec![batch.clone()], exact).is_ok());

        let smaller = ByteSize::from_bytes(size - 1);
        let error = recompute_scan_nested_sets(vec![batch], smaller).unwrap_err();
        assert2::assert!(error.to_string().contains("scan result too large to merge"));
    }

    #[test]
    fn span_store_constructor_preserves_scan_concat_default() {
        let blocks = Arc::new(BlockStore::new(
            Arc::new(InMemory::new()),
            Url::parse("memory:///").unwrap(),
        ));
        let store = KrabkaSpanStore::new(blocks, shared(TraceIndex::new()), None);

        assert2::assert!(store.scan_concat_max == DEFAULT_SCAN_CONCAT_MAX);
    }

    fn dictionary_attr_batch() -> RecordBatch {
        let mut fields = test_schema()
            .fields()
            .iter()
            .map(|field| field.as_ref().clone())
            .collect::<Vec<_>>();
        fields.push(Field::new(
            format!("{ATTR_PREFIX}http.method"),
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        ));
        let schema = Arc::new(Schema::new(fields));
        let base = batch();
        let mut columns = base.columns().to_vec();
        let mut methods = StringDictionaryBuilder::<Int32Type>::new();
        methods.append_value("GET");
        methods.append_value("POST");
        columns.push(Arc::new(methods.finish()) as ArrayRef);
        RecordBatch::try_new(schema, columns).unwrap()
    }

    /// `nested_string_attrs` pairs a row's attribute keys with the first
    /// value of each key's value list, skipping anything incomplete. Its skip
    /// conditions are `||` pairs that survived because no fixture made the
    /// two halves disagree, so every skip below fires for exactly one reason.
    ///
    /// Two further properties need shapes a naive fixture does not have: one
    /// key has *two* values, without which taking the first and taking the
    /// last are the same read; and there is one more key than value list,
    /// without which pairing by the shorter and by the longer agree.
    #[test]
    fn nested_attributes_pair_each_key_with_its_first_value() {
        fn list(values: &mut ListBuilder<ListBuilder<StringBuilder>>, entries: &[Option<&str>]) {
            for entry in entries {
                match entry {
                    Some(value) => values.values().values().append_value(value),
                    None => values.values().values().append_null(),
                }
            }
            values.values().append(true);
        }

        let mut keys = ListBuilder::new(StringBuilder::new());
        for key in ["a", "", "c", "d", "e", "f"] {
            if key.is_empty() {
                keys.values().append_null();
            } else {
                keys.values().append_value(key);
            }
        }
        keys.append(true);
        keys.append_null();
        let keys = keys.finish();
        let mut values = ListBuilder::new(ListBuilder::new(StringBuilder::new()));
        list(&mut values, &[Some("1")]);
        list(&mut values, &[Some("2")]);
        list(&mut values, &[]);
        list(&mut values, &[None]);
        list(&mut values, &[Some("first"), Some("second")]);
        // Five value lists against six keys, so pairing by the longer runs
        // off the end.
        values.append(true);
        // A second row, whose keys list is null while this one is not.
        list(&mut values, &[Some("z")]);
        values.append(true);
        let values = values.finish();

        let attrs = |row| super::nested_string_attrs(&keys, &values, row).expect("the row reads");

        // "" has a null key, "c" an empty value list, "d" a null first value,
        // and "f" no value list at all. Each is skipped for its own reason,
        // so loosening any one guard admits a different spurious pair.
        check!(
            attrs(0)
                == vec![
                    ("a".to_string(), AttrValue::Str("1".to_string())),
                    ("e".to_string(), AttrValue::Str("first".to_string())),
                ]
        );

        // A row whose keys are null yields nothing, whatever the values hold.
        check!(attrs(1).is_empty());
    }

    /// A null list row still carries offsets, and Arrow does not require them
    /// to be empty -- a builder always writes an empty range, but the format
    /// permits a null row to span real values, and a Parquet reader may hand
    /// one over. So the row-level null check must be honoured rather than
    /// inferred from the row reading as empty.
    ///
    /// This is the only shape that separates the two halves of that check:
    /// with a builder-made array, reading through a null row yields nothing
    /// anyway, and skipping it or walking it give the same empty answer.
    #[test]
    fn a_null_attribute_row_is_skipped_even_when_it_spans_values() {
        let item = |name, data_type| Arc::new(Field::new(name, data_type, true));

        // Row 0 is null, yet its offsets cover the single key "x".
        let keys = ListArray::new(
            item("item", DataType::Utf8),
            OffsetBuffer::new(vec![0, 1].into()),
            Arc::new(StringArray::from(vec!["x"])),
            Some(NullBuffer::from(vec![false])),
        );

        // The matching values row is present and well formed, so the only
        // reason to skip is the null flag on the keys.
        let mut values = ListBuilder::new(ListBuilder::new(StringBuilder::new()));
        values.values().values().append_value("v");
        values.values().append(true);
        values.append(true);
        let values = values.finish();

        check!(
            super::nested_string_attrs(&keys, &values, 0)
                .expect("the row reads")
                .is_empty(),
            "a null keys row contributes nothing, whatever its offsets span"
        );
    }

    /// The typed column readers each downcast a column and return one cell.
    /// Every one of them survived as a constant, so the values are chosen to
    /// avoid the constants: the int column is read at row 1, because row 0
    /// holds 1, which is itself one of the answers a collapsed body gives.
    #[test]
    fn the_typed_column_readers_return_their_own_cell_or_refuse_the_column() {
        let batch = typed_attr_batch();
        let column = |name: &str| {
            batch
                .column_by_name(&format!("{ATTR_PREFIX}{name}"))
                .expect("the column is present")
                .clone()
        };

        check!(super::int64_array_value(column("int").as_ref(), 1).expect("an int column") == 2);
        check!(
            (super::float64_array_value(column("float").as_ref(), 0).expect("a float column")
                - 1.5)
                .abs()
                < f64::EPSILON
        );
        // Both rows, since true and false are each a constant a collapsed
        // body returns and neither alone rules the other out.
        check!(super::bool_array_value(column("bool").as_ref(), 0).expect("a bool column"));
        check!(!super::bool_array_value(column("bool").as_ref(), 1).expect("a bool column"));
        check!(
            super::string_array_value(column("str").as_ref(), 1).expect("a string column") == "two"
        );

        // A column of the wrong type is refused rather than reinterpreted.
        check!(super::int64_array_value(column("str").as_ref(), 0).is_err());
        check!(super::float64_array_value(column("int").as_ref(), 0).is_err());
        check!(super::bool_array_value(column("int").as_ref(), 0).is_err());

        // By name: the cell is read from the named column, and an absent name
        // is an error rather than a default.
        check!(super::int64_value(&batch, &format!("{ATTR_PREFIX}int"), 1).expect("by name") == 2);
        // The error must name the absent column: falling back to some other
        // column also fails, but for the wrong reason and with the wrong
        // message, which is the only thing separating the two.
        check!(
            super::int64_value(&batch, "no.such.column", 0)
                .expect_err("an absent column is an error")
                .to_string()
                .contains("no.such.column"),
            "the error names the column that is missing"
        );
    }

    /// `nullable_fixed_value` reads one fixed-width binary cell, or None when
    /// it is null. The trace id is neither all-zero nor all-one, so a body
    /// collapsed to either constant is distinguishable from a real read.
    #[test]
    fn a_nullable_fixed_column_reads_its_own_row_or_none() {
        let batch = batch();
        let trace_id = |row| {
            super::nullable_fixed_value::<16>(&batch, COL_TRACE_ID, row)
                .expect("the trace id column is readable")
        };

        check!(trace_id(0) == Some([7; 16]));
        check!(
            trace_id(1) == Some([9; 16]),
            "and each row reads its own cell"
        );
        check!(
            super::nullable_fixed_value::<8>(&batch, COL_PARENT_SPAN_ID, 0)
                .expect("the parent column is readable")
                .is_none(),
            "a null cell is None, not a zeroed id"
        );

        // A width that disagrees with the column is an error rather than a
        // silent truncation, and an absent column is an error too.
        check!(super::nullable_fixed_value::<4>(&batch, COL_TRACE_ID, 0).is_err());
        check!(super::nullable_fixed_value::<16>(&batch, "no.such.column", 0).is_err());
    }

    /// A batch carrying one promoted attribute column of every type the
    /// reader supports, alongside the three kinds it must skip: a column
    /// type it does not handle, a null cell, and a dictionary whose values
    /// are not strings.
    fn typed_attr_batch() -> RecordBatch {
        let mut fields = test_schema()
            .fields()
            .iter()
            .map(|field| field.as_ref().clone())
            .collect::<Vec<_>>();
        let mut columns = batch().columns().to_vec();

        let mut dict = StringDictionaryBuilder::<Int32Type>::new();
        dict.append_value("GET");
        dict.append_value("POST");
        // A dictionary of integers, not strings. It must be skipped: the
        // string reader would misread it.
        let mut int_dict = PrimitiveDictionaryBuilder::<Int32Type, Int64Type>::new();
        int_dict.append_value(7);
        int_dict.append_value(8);

        let string_dict = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        let integer_dict =
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Int64));
        let typed: Vec<(&str, DataType, ArrayRef)> = vec![
            (
                "str",
                DataType::Utf8,
                Arc::new(StringArray::from(vec!["one", "two"])),
            ),
            ("dict", string_dict, Arc::new(dict.finish())),
            (
                "int",
                DataType::Int64,
                Arc::new(Int64Array::from(vec![1, 2])),
            ),
            (
                "float",
                DataType::Float64,
                Arc::new(Float64Array::from(vec![1.5, 2.5])),
            ),
            (
                "bool",
                DataType::Boolean,
                Arc::new(BooleanArray::from(vec![true, false])),
            ),
            (
                "unsupported",
                DataType::Int32,
                Arc::new(Int32Array::from(vec![7, 8])),
            ),
            ("intdict", integer_dict, Arc::new(int_dict.finish())),
            // Null in row 0 only, so the skip is provably per-row.
            (
                "nullable",
                DataType::Utf8,
                Arc::new(StringArray::from(vec![None, Some("x")])),
            ),
        ];
        for (name, data_type, column) in typed {
            fields.push(Field::new(format!("{ATTR_PREFIX}{name}"), data_type, true));
            columns.push(column);
        }
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .expect("the typed attribute batch is well formed")
    }

    /// `attr_values_with_resource` reads one row's promoted attribute
    /// columns, turning each into the `AttrValue` its Arrow type implies.
    /// Every supported type is pinned to a value of that type, since an arm
    /// borrowed from a neighbouring arm still produces a well-formed pair
    /// and only the variant gives it away.
    #[test]
    fn promoted_attributes_take_the_value_type_their_column_declares() {
        let batch = typed_attr_batch();
        let row = |index| {
            super::attr_values_with_resource(&batch, index, false).expect("the row is readable")
        };
        let str_value = |value: &str| AttrValue::Str(value.to_string());

        // Row 0. `unsupported`, `intdict` and `nullable` are all absent:
        // an unhandled column type, a non-string dictionary, and a null cell.
        check!(
            row(0)
                == vec![
                    ("svc".to_string(), str_value("a")),
                    ("str".to_string(), str_value("one")),
                    ("dict".to_string(), str_value("GET")),
                    ("int".to_string(), AttrValue::Int(1)),
                    ("float".to_string(), AttrValue::Float(1.5)),
                    ("bool".to_string(), AttrValue::Bool(true)),
                ]
        );

        // Row 1 differs in every value, so an arm that ignores its row index
        // is caught, and `nullable` now appears -- the null skip is per-cell,
        // not a decision made once for the whole column.
        check!(
            row(1)
                == vec![
                    ("svc".to_string(), str_value("b")),
                    ("str".to_string(), str_value("two")),
                    ("dict".to_string(), str_value("POST")),
                    ("int".to_string(), AttrValue::Int(2)),
                    ("float".to_string(), AttrValue::Float(2.5)),
                    ("bool".to_string(), AttrValue::Bool(false)),
                    ("nullable".to_string(), str_value("x")),
                ]
        );
    }

    fn resource_service_matcher(op: MatchCmp, value: MatchValue) -> SpanMatcher {
        SpanMatcher {
            scope: MatchScope::Resource,
            key: "service.name".into(),
            op,
            value,
            negated: false,
        }
    }

    #[test]
    fn resource_matches_service_name_uses_root_service_column() {
        let batch = batch();

        for (i, (matcher, want)) in [
            (
                resource_service_matcher(MatchCmp::Eq, MatchValue::Str("api".into())),
                true,
            ),
            (
                resource_service_matcher(MatchCmp::Eq, MatchValue::Str("web".into())),
                false,
            ),
            (
                resource_service_matcher(MatchCmp::Neq, MatchValue::Nil),
                true,
            ),
            (
                SpanMatcher {
                    scope: MatchScope::Resource,
                    key: "missing".into(),
                    op: MatchCmp::Neq,
                    value: MatchValue::Nil,
                    negated: false,
                },
                false,
            ),
        ]
        .into_iter()
        .enumerate()
        {
            check!(
                resource_matches(&batch, 0, &matcher).unwrap() == want,
                "case {i}"
            );
        }
    }

    #[test]
    fn root_service_matches_preserves_nil_and_string_semantics() {
        for (i, (service, matcher, want)) in [
            (
                "api",
                resource_service_matcher(MatchCmp::Eq, MatchValue::Str("api".into())),
                true,
            ),
            (
                "api",
                resource_service_matcher(MatchCmp::Eq, MatchValue::Str("web".into())),
                false,
            ),
            (
                "api",
                resource_service_matcher(MatchCmp::Neq, MatchValue::Nil),
                true,
            ),
            (
                "api",
                resource_service_matcher(MatchCmp::Eq, MatchValue::Nil),
                false,
            ),
            (
                "",
                resource_service_matcher(MatchCmp::Eq, MatchValue::Nil),
                true,
            ),
            (
                "",
                resource_service_matcher(MatchCmp::Neq, MatchValue::Nil),
                false,
            ),
        ]
        .into_iter()
        .enumerate()
        {
            check!(
                root_service_matches(service, &matcher) == want,
                "case {i}: service {service:?}"
            );
        }
    }

    #[test]
    fn reconstructs_trace_from_candidate_batches() {
        let got = trace_from_batches(&[7; 16], vec![batch()])
            .unwrap()
            .unwrap();
        check!(
            (
                got.root_service_name.as_str(),
                got.spans.len(),
                got.spans[0].attributes.as_slice(),
            ) == (
                "api",
                1,
                [("svc".into(), AttrValue::Str("a".into()))].as_slice(),
            )
        );
    }

    #[test]
    fn reconstructs_trace_from_dictionary_promoted_attr_columns() {
        let got = trace_from_batches(&[7; 16], vec![dictionary_attr_batch()])
            .unwrap()
            .unwrap();
        assert2::assert!(
            got.spans[0]
                .attributes
                .iter()
                .any(|(key, value)| key == "http.method" && value == &AttrValue::Str("GET".into()))
        );
    }

    #[test]
    fn generic_attrs_do_not_duplicate_promoted_attr_columns() {
        let span = span_with_nested_refs();
        let batch = span_batch_with_promoted_attrs(
            std::slice::from_ref(&span),
            &[PromotedSpanAttr::int("http.status_code")],
        )
        .unwrap();
        let got = trace_from_batches(&span.trace_id, vec![batch])
            .unwrap()
            .unwrap();
        assert2::assert!(
            got.spans[0]
                .attributes
                .iter()
                .filter(|(key, _)| key == "http.status_code")
                .count()
                == 1
        );
    }

    #[test]
    fn cold_intrinsic_values_include_child_count_and_instrumentation() {
        let batches = vec![batch()];
        check!(
            intrinsic_values_from_batches("span:childCount", &batches)
                .unwrap()
                .iter()
                .any(|value| value.type_ == "int" && value.value == "0")
        );
        check!(
            intrinsic_values_from_batches("instrumentation:name", &batches)
                .unwrap()
                .iter()
                .any(|value| value.type_ == "string" && value.value == "tracer")
        );
        check!(
            intrinsic_values_from_batches("instrumentation:version", &batches)
                .unwrap()
                .iter()
                .any(|value| value.type_ == "string")
        );
    }

    #[test]
    fn child_count_is_per_trace_in_multi_trace_scan_batch() {
        // Two traces in ONE scan batch, each root -> child. Per-trace nested-set
        // numbering resets `left` to 1, so both roots get left=1 and both
        // children get parent_id=1. `span:childCount` must be counted PER TRACE
        // (1 each), not across the whole batch (which collides on left=1 and
        // inflates each root to 2).
        let schema = test_schema();
        let mut trace_id = FixedSizeBinaryBuilder::with_capacity(4, 16);
        for t in [[7_u8; 16], [7; 16], [9; 16], [9; 16]] {
            trace_id.append_value(t).unwrap();
        }
        let mut span_id = FixedSizeBinaryBuilder::with_capacity(4, 8);
        for s in [[1_u8; 8], [2; 8], [1; 8], [2; 8]] {
            span_id.append_value(s).unwrap();
        }
        let mut parent = FixedSizeBinaryBuilder::with_capacity(4, 8);
        parent.append_null(); // trace A root
        parent.append_value([1; 8]).unwrap(); // trace A child -> A root
        parent.append_null(); // trace B root
        parent.append_value([1; 8]).unwrap(); // trace B child -> B root
        let s4 = |a: &str, b: &str| StringArray::from(vec![a, a, b, b]);
        let i32_4 = || Int32Array::from(vec![0, 0, 0, 0]);
        let i64_4 = || Int64Array::from(vec![0_i64, 0, 0, 0]);
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(trace_id.finish()) as ArrayRef,
                Arc::new(span_id.finish()),
                Arc::new(parent.finish()),
                Arc::new(i32_4()), // ns_left (recomputed)
                Arc::new(i32_4()), // ns_right (recomputed)
                Arc::new(i32_4()), // parent_id (recomputed)
                Arc::new(i32_4()), // child_count (recomputed)
                Arc::new(s4("api", "web")),
                Arc::new(s4("GET /", "GET /x")),
                Arc::new(i64_4()),
                Arc::new(i64_4()),
                Arc::new(StringArray::from(vec!["root", "child", "root", "child"])),
                Arc::new(i32_4()),
                Arc::new(i64_4()),
                Arc::new(i64_4()),
                Arc::new(i32_4()),
                Arc::new(s4("", "")),
                Arc::new(s4("tracer", "tracer")),
                Arc::new(s4("", "")),
                Arc::new(s4("a", "b")),
            ],
        )
        .unwrap();

        let out = recompute_batch_nested_sets(&batch).unwrap();
        // Per-trace `left` reset (collision confirms the multi-trace scenario).
        for (row, want) in [(0, 1), (2, 1)] {
            check!(
                int32_value(&out, COL_NS_LEFT, row).unwrap() == want,
                "row {row}"
            );
        }
        // Each root has exactly one child; children have none — NOT inflated to 2.
        for (row, want) in [(0, 1), (1, 0), (2, 1), (3, 0)] {
            check!(
                int32_value(&out, COL_CHILD_COUNT, row).unwrap() == want,
                "row {row}"
            );
        }
        // Roots encode nestedSetParent = -1 (Tempo no-parent sentinel) so the
        // Drilldown's `nestedSetParent < 0` primary signal selects them; each
        // child points at its root's `left` (1 after the per-trace reset).
        for (row, want) in [(0, -1), (1, 1), (2, -1), (3, 1)] {
            check!(
                int32_value(&out, COL_PARENT_ID, row).unwrap() == want,
                "row {row}"
            );
        }
    }

    #[test]
    fn metrics_by_attr_materializes_span_and_resource_columns() {
        // A `by(span.<attr>)` / `by(resource.<attr>)` projection must materialize
        // an `attr.<key>` column from the parquet attr arrays so grouping can read
        // it; spans lacking the attribute become the nil group (NULL). The
        // in-memory store can't catch this (it materializes every attr), so this
        // exercises the production parquet batch path directly.
        use crate::span::{AttrValue as SAttr, KeyValue, Span, SpanKind, StatusCode};
        let mk = |id: u8, parent: Option<u8>, span_attrs: Vec<KeyValue>, version: Option<&str>| {
            let mut resource_attrs = vec![KeyValue {
                key: "service.name".into(),
                value: SAttr::Str("api".into()),
            }];
            if let Some(v) = version {
                resource_attrs.push(KeyValue {
                    key: "service.version".into(),
                    value: SAttr::Str(v.into()),
                });
            }
            Span {
                trace_id: [7; 16],
                span_id: [id; 8],
                parent_span_id: parent.map(|p| [p; 8]),
                name: "GET /".into(),
                kind: SpanKind::Server,
                start_ns: 1_000 + i64::from(id),
                duration_ns: 100,
                status: StatusCode::Ok,
                status_message: String::new(),
                resource_attrs,
                span_attrs,
                events: vec![],
                links: vec![],
                instrumentation_scope: "otel-rust".into(),
                instrumentation_version: "1.2.3".into(),
            }
        };
        let root = mk(
            1,
            None,
            vec![
                KeyValue {
                    key: "http.method".into(),
                    value: SAttr::Str("GET".into()),
                },
                KeyValue {
                    key: format!("{INSTRUMENTATION_ATTR_PREFIX}library"),
                    value: SAttr::Str("otel".into()),
                },
            ],
            Some("1.2.3"),
        );
        let child = mk(2, Some(1), vec![], None);
        let batch = span_batch(&[root, child]).unwrap();

        let matcher = |scope, key: &str| SpanMatcher {
            scope,
            key: key.into(),
            op: MatchCmp::Neq,
            value: MatchValue::Nil,
            negated: false,
        };
        let out = add_span_attr_columns(
            vec![batch],
            &[
                matcher(MatchScope::Span, "http.method"),
                matcher(MatchScope::Resource, "service.version"),
                matcher(MatchScope::Instrumentation, "library"),
            ],
        )
        .unwrap();
        let out = &out[0];

        let sorted = |name: &str| -> Vec<Option<String>> {
            let col = out
                .column_by_name(name)
                .unwrap_or_else(|| panic!("{name} materialized"));
            let mut vals: Vec<Option<String>> = (0..out.num_rows())
                .map(|row| {
                    if col.is_null(row) {
                        None
                    } else {
                        Some(string_array_value(col.as_ref(), row).unwrap())
                    }
                })
                .collect();
            vals.sort();
            vals
        };
        // The span with the attribute carries its value; the other is the nil
        // group (NULL → empty label downstream).
        for (_name, column, expected) in [
            (
                "span method",
                "attr.http.method",
                vec![None, Some("GET".to_string())],
            ),
            (
                "service version",
                "attr.service.version",
                vec![None, Some("1.2.3".to_string())],
            ),
            (
                "instrumentation library",
                "attr.__instrumentation.library",
                vec![None, Some("otel".to_string())],
            ),
        ] {
            assert2::assert!(sorted(column) == expected);
        }

        let accepts = |key: &str, value: &str| {
            instrumentation_matches(
                out,
                0,
                &SpanMatcher {
                    scope: MatchScope::Instrumentation,
                    key: key.into(),
                    op: MatchCmp::Eq,
                    value: MatchValue::Str(value.into()),
                    negated: false,
                },
            )
            .unwrap()
        };
        assert2::assert!(accepts("name", "otel-rust"));
        assert2::assert!(accepts("version", "1.2.3"));
        assert2::assert!(accepts("library", "otel"));
        assert2::assert!(!accepts("name", "other"));

        let mut values = BTreeSet::new();
        collect_attribute_tag_values(out, "http.method", "service.version", &mut values).unwrap();
        assert2::assert!(
            values
                == BTreeSet::from([
                    ("string".into(), "1.2.3".into()),
                    ("string".into(), "GET".into()),
                ])
        );
        values.clear();
        collect_attribute_tag_values(
            out,
            "instrumentation.library",
            &format!("{INSTRUMENTATION_ATTR_PREFIX}library"),
            &mut values,
        )
        .unwrap();
        assert2::assert!(values == BTreeSet::from([("string".into(), "otel".into())]));
    }

    #[tokio::test]
    async fn empty_store_scans_as_empty_span_table() {
        let blocks = Arc::new(BlockStore::new(
            Arc::new(InMemory::new()),
            Url::parse("memory:///").unwrap(),
        ));
        let store = KrabkaSpanStore::new(blocks, shared(TraceIndex::new()), None);
        let scan = store.scan("tenant", &[], 0, 10).await.unwrap();
        let rows: usize = scan
            .ctx
            .table(&scan.span_table)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap()
            .iter()
            .map(RecordBatch::num_rows)
            .sum();
        assert2::assert!(rows == 0);
    }

    #[tokio::test]
    async fn tag_discovery_unions_cold_index_values() {
        let object_store = Arc::new(InMemory::new());
        let blocks = Arc::new(BlockStore::new(
            object_store.clone(),
            Url::parse("memory:///").unwrap(),
        ));
        let writer = BlockWriter::new(object_store);
        let span = span_with_nested_refs();
        let batch = span_batch(std::slice::from_ref(&span)).unwrap();
        let meta = writer
            .write_block_with_decl(
                "tenant",
                "blocks/tags.parquet",
                span_block_schema(),
                &[batch],
                &span_block_decl(),
                SummaryColumns::new(SCOL_TRACE_ID, SCOL_START_NANO),
            )
            .await
            .unwrap();
        let mut index = TraceIndex::new();
        let mut bloom = ShardedTraceBloom::with_tempo_defaults(1);
        bloom.insert(&span.trace_id);
        let mut tags = BTreeSet::new();
        tags.insert("service.name".to_string());
        let mut values = BTreeMap::new();
        values.insert(
            "service.name".to_string(),
            BTreeSet::from(["api".to_string()]),
        );
        index.add_trace_block(
            "tenant",
            TraceBlockStats {
                object_key: meta.object_key,
                min_ts: meta.min_ts,
                max_ts: meta.max_ts,
                bloom,
                tag_names: tags,
                tag_values: values,
            },
        );

        let store = KrabkaSpanStore::new(blocks, shared(index), None);
        assert2::assert!(
            store.tag_names("tenant", None, 0, 10_000).await.unwrap()[0]
                .tags
                .clone()
                == vec!["service.name".to_string()]
        );
        assert2::assert!(
            store
                .tag_values("tenant", "service.name", 0, 10_000)
                .await
                .unwrap()[0]
                .value
                .clone()
                == "api".to_string()
        );
    }

    #[tokio::test]
    async fn cold_attribute_tag_values_preserve_static_types() {
        let object_store = Arc::new(InMemory::new());
        let blocks = Arc::new(BlockStore::new(
            object_store.clone(),
            Url::parse("memory:///").unwrap(),
        ));
        let writer = BlockWriter::new(object_store);
        let span = span_with_nested_refs();
        let batch = span_batch(std::slice::from_ref(&span)).unwrap();
        let meta = writer
            .write_block_with_decl(
                "tenant",
                "blocks/typed-tags.parquet",
                span_block_schema(),
                &[batch],
                &span_block_decl(),
                SummaryColumns::new(SCOL_TRACE_ID, SCOL_START_NANO),
            )
            .await
            .unwrap();
        let mut bloom = ShardedTraceBloom::with_tempo_defaults(1);
        bloom.insert(&span.trace_id);
        let mut index = TraceIndex::new();
        index.add_trace_block(
            "tenant",
            TraceBlockStats {
                object_key: meta.object_key,
                min_ts: meta.min_ts,
                max_ts: meta.max_ts,
                bloom,
                tag_names: BTreeSet::from([
                    "http.status_code".to_string(),
                    "retryable".to_string(),
                ]),
                tag_values: BTreeMap::from([
                    (
                        "http.status_code".to_string(),
                        BTreeSet::from(["504".to_string()]),
                    ),
                    (
                        "retryable".to_string(),
                        BTreeSet::from(["true".to_string()]),
                    ),
                ]),
            },
        );
        let store = KrabkaSpanStore::new(blocks, shared(index), None);

        let status_values = store
            .tag_values("tenant", "http.status_code", 0, 10_000)
            .await
            .unwrap();
        let retryable_values = store
            .tag_values("tenant", "retryable", 0, 10_000)
            .await
            .unwrap();

        assert2::assert!(
            status_values
                == vec![TypedValue {
                    type_: "int".into(),
                    value: "504".into(),
                }]
        );
        assert2::assert!(
            retryable_values
                == vec![TypedValue {
                    type_: "bool".into(),
                    value: "true".into(),
                }]
        );
    }

    #[tokio::test]
    async fn cold_nested_tag_values_scan_event_and_link_attributes() {
        let object_store = Arc::new(InMemory::new());
        let blocks = Arc::new(BlockStore::new(
            object_store.clone(),
            Url::parse("memory:///").unwrap(),
        ));
        let writer = BlockWriter::new(object_store);
        let span = span_with_nested_refs();
        let batch = span_batch(std::slice::from_ref(&span)).unwrap();
        let meta = writer
            .write_block_with_decl(
                "tenant",
                "blocks/nested-tag-values.parquet",
                span_block_schema(),
                &[batch],
                &span_block_decl(),
                SummaryColumns::new(SCOL_TRACE_ID, SCOL_START_NANO),
            )
            .await
            .unwrap();
        let mut bloom = ShardedTraceBloom::with_tempo_defaults(1);
        bloom.insert(&span.trace_id);
        let mut index = TraceIndex::new();
        index.add_trace_block(
            "tenant",
            TraceBlockStats {
                object_key: meta.object_key,
                min_ts: meta.min_ts,
                max_ts: meta.max_ts,
                bloom,
                tag_names: BTreeSet::from(["exception.type".into(), "link.kind".into()]),
                tag_values: BTreeMap::from([
                    ("exception.type".into(), BTreeSet::from(["timeout".into()])),
                    ("link.kind".into(), BTreeSet::from(["retry".into()])),
                ]),
            },
        );
        let store = KrabkaSpanStore::new(blocks, shared(index), None);

        let event_values = store
            .tag_values("tenant", "exception.type", 0, 10_000)
            .await
            .unwrap();
        let scoped_event_values = store
            .tag_values("tenant", "event.exception.type", 0, 10_000)
            .await
            .unwrap();
        let link_values = store
            .tag_values("tenant", "link.kind", 0, 10_000)
            .await
            .unwrap();
        let scoped_link_values = store
            .tag_values("tenant", "link.link.kind", 0, 10_000)
            .await
            .unwrap();

        check!(
            event_values
                == vec![TypedValue {
                    type_: "string".into(),
                    value: "timeout".into(),
                }]
        );
        check!(scoped_event_values == event_values);
        check!(
            link_values
                == vec![TypedValue {
                    type_: "string".into(),
                    value: "retry".into(),
                }]
        );
        check!(scoped_link_values == link_values);
    }

    #[tokio::test]
    async fn cold_nested_tag_names_scan_event_and_link_attributes() {
        let object_store = Arc::new(InMemory::new());
        let blocks = Arc::new(BlockStore::new(
            object_store.clone(),
            Url::parse("memory:///").unwrap(),
        ));
        let writer = BlockWriter::new(object_store);
        let span = span_with_nested_refs();
        let batch = span_batch(std::slice::from_ref(&span)).unwrap();
        let meta = writer
            .write_block_with_decl(
                "tenant",
                "blocks/nested-tag-names.parquet",
                span_block_schema(),
                &[batch],
                &span_block_decl(),
                SummaryColumns::new(SCOL_TRACE_ID, SCOL_START_NANO),
            )
            .await
            .unwrap();
        let mut bloom = ShardedTraceBloom::with_tempo_defaults(1);
        bloom.insert(&span.trace_id);
        let mut index = TraceIndex::new();
        index.add_trace_block(
            "tenant",
            TraceBlockStats {
                object_key: meta.object_key,
                min_ts: meta.min_ts,
                max_ts: meta.max_ts,
                bloom,
                tag_names: BTreeSet::from(["exception.type".into(), "link.kind".into()]),
                tag_values: BTreeMap::new(),
            },
        );
        let store = KrabkaSpanStore::new(blocks, shared(index), None);

        let event_tags = store
            .tag_names("tenant", Some(TagScope::Event), 0, 10_000)
            .await
            .unwrap();
        let link_tags = store
            .tag_names("tenant", Some(TagScope::Link), 0, 10_000)
            .await
            .unwrap();

        check!(
            event_tags
                == vec![ScopedTag {
                    scope: TagScope::Event,
                    tags: vec![
                        "event:name".to_string(),
                        "event:timeSinceStart".to_string(),
                        "exception.type".to_string(),
                    ],
                }]
        );
        check!(
            link_tags
                == vec![ScopedTag {
                    scope: TagScope::Link,
                    tags: vec![
                        "link.kind".to_string(),
                        "link:spanID".to_string(),
                        "link:traceID".to_string(),
                    ],
                }]
        );
    }

    #[tokio::test]
    async fn cold_tag_discovery_exposes_static_traceql_scopes() {
        let blocks = Arc::new(BlockStore::new(
            Arc::new(InMemory::new()),
            Url::parse("memory:///").unwrap(),
        ));
        let mut index = TraceIndex::new();
        index.add_trace_block(
            "tenant",
            TraceBlockStats {
                object_key: "blocks/none.parquet".into(),
                min_ts: 0,
                max_ts: 10,
                bloom: ShardedTraceBloom::new(1, 8, 0.01),
                tag_names: BTreeSet::new(),
                tag_values: BTreeMap::new(),
            },
        );

        let store = KrabkaSpanStore::new(blocks, shared(index), None);

        let intrinsic = store
            .tag_names("tenant", Some(TagScope::Intrinsic), 0, 10)
            .await
            .unwrap();
        check!(
            (
                intrinsic.len(),
                intrinsic[0].scope,
                intrinsic[0].tags.contains(&"span:duration".to_string()),
                intrinsic[0].tags.contains(&"trace:id".to_string()),
            ) == (1, TagScope::Intrinsic, true, true)
        );

        let event = store
            .tag_names("tenant", Some(TagScope::Event), 0, 10)
            .await
            .unwrap();
        check!(
            event
                .iter()
                .map(|entry| (&entry.scope, &entry.tags))
                .collect::<Vec<_>>()
                == vec![(
                    &TagScope::Event,
                    &vec!["event:name".into(), "event:timeSinceStart".into()]
                )]
        );

        let link = store
            .tag_names("tenant", Some(TagScope::Link), 0, 10)
            .await
            .unwrap();
        check!(
            link.iter()
                .map(|entry| (&entry.scope, &entry.tags))
                .collect::<Vec<_>>()
                == vec![(
                    &TagScope::Link,
                    &vec!["link:spanID".into(), "link:traceID".into()]
                )]
        );

        let instrumentation = store
            .tag_names("tenant", Some(TagScope::Instrumentation), 0, 10)
            .await
            .unwrap();
        check!(
            instrumentation
                .iter()
                .map(|entry| (&entry.scope, &entry.tags))
                .collect::<Vec<_>>()
                == vec![(
                    &TagScope::Instrumentation,
                    &vec![
                        "instrumentation:name".into(),
                        "instrumentation:version".into(),
                    ]
                )]
        );
    }

    #[tokio::test]
    async fn cold_span_tag_discovery_excludes_intrinsic_index_names() {
        let blocks = Arc::new(BlockStore::new(
            Arc::new(InMemory::new()),
            Url::parse("memory:///").unwrap(),
        ));
        let mut index = TraceIndex::new();
        index.add_trace_block(
            "tenant",
            TraceBlockStats {
                object_key: "blocks/none.parquet".into(),
                min_ts: 0,
                max_ts: 10,
                bloom: ShardedTraceBloom::new(1, 8, 0.01),
                tag_names: BTreeSet::from([
                    "http.method".to_string(),
                    "event:name".to_string(),
                    "instrumentation:name".to_string(),
                ]),
                tag_values: BTreeMap::new(),
            },
        );
        let store = KrabkaSpanStore::new(blocks, shared(index), None);

        let tags = store
            .tag_names("tenant", Some(TagScope::Span), 0, 10)
            .await
            .unwrap();

        assert2::assert!(
            tags == vec![ScopedTag {
                scope: TagScope::Span,
                tags: vec!["http.method".to_string()],
            }]
        );
    }

    #[tokio::test]
    async fn cold_scan_can_read_one_backend_row_group_job() {
        let object_store = Arc::new(InMemory::new());
        let blocks = Arc::new(BlockStore::new(
            object_store.clone(),
            Url::parse("memory:///").unwrap(),
        ));
        let first = encode_span_rows(&[block_attr_span_row(
            [1; 16],
            [1; 8],
            "first-rg",
            false,
            vec!["GET".into()],
        )])
        .unwrap();
        let second = encode_span_rows(&[block_attr_span_row(
            [2; 16],
            [2; 8],
            "second-rg",
            false,
            vec!["POST".into()],
        )])
        .unwrap();
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(1))
            .set_write_batch_size(1)
            .build();
        let object_writer = BufWriter::new(
            object_store.clone(),
            Path::from("blocks/row-groups.parquet"),
        );
        let mut writer =
            AsyncArrowWriter::try_new(object_writer, span_block_schema(), Some(props)).unwrap();
        writer.write(&first).await.unwrap();
        writer.write(&second).await.unwrap();
        writer.close().await.unwrap();

        let index = || {
            let mut index = TraceIndex::new();
            index.add_trace_block(
                "tenant",
                TraceBlockStats {
                    object_key: "blocks/row-groups.parquet".into(),
                    min_ts: 0,
                    max_ts: 10,
                    bloom: ShardedTraceBloom::with_tempo_defaults(1),
                    tag_names: BTreeSet::new(),
                    tag_values: BTreeMap::new(),
                },
            );
            index
        };
        let capped_blocks = Arc::new(BlockStore::new_with_block_read_max(
            object_store,
            Url::parse("memory:///").unwrap(),
            krabka_units::bytes(1),
        ));
        let capped_store = KrabkaSpanStore::new(capped_blocks, shared(index()), None);
        let store = KrabkaSpanStore::new(blocks, shared(index()), None);

        let options = ScanOptions {
            job: Some(ScanJob {
                object_key: "blocks/row-groups.parquet".into(),
                row_group_start: 1,
                row_group_end: 2,
            }),
            ..ScanOptions::default()
        };
        let scan = store
            .scan_with_options("tenant", &[], 0, 10, &options)
            .await
            .unwrap();
        let batches = collect_table(&scan.ctx, &scan.span_table).await.unwrap();
        let names = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column_by_name(krabka_traceql::COL_NAME)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .iter()
                    .map(|value| value.unwrap().to_string())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();

        assert2::assert!(names == vec!["second-rg"]);
        assert2::assert!(
            capped_store
                .scan_with_options("tenant", &[], 0, 10, &options)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn cold_scan_rejects_backend_row_group_job_for_other_tenant() {
        let object_store = Arc::new(InMemory::new());
        let blocks = Arc::new(BlockStore::new(
            object_store.clone(),
            Url::parse("memory:///").unwrap(),
        ));
        let batch = encode_span_rows(&[block_attr_span_row(
            [1; 16],
            [1; 8],
            "tenant-a-only",
            false,
            vec!["GET".into()],
        )])
        .unwrap();
        let object_writer = BufWriter::new(
            object_store.clone(),
            Path::from("blocks/tenant-a-row-groups.parquet"),
        );
        let mut writer =
            AsyncArrowWriter::try_new(object_writer, span_block_schema(), None).unwrap();
        writer.write(&batch).await.unwrap();
        writer.close().await.unwrap();

        let mut index = TraceIndex::new();
        index.add_trace_block(
            "tenant-a",
            TraceBlockStats {
                object_key: "blocks/tenant-a-row-groups.parquet".into(),
                min_ts: 0,
                max_ts: 10,
                bloom: ShardedTraceBloom::with_tempo_defaults(1),
                tag_names: BTreeSet::new(),
                tag_values: BTreeMap::new(),
            },
        );
        let store = KrabkaSpanStore::new(blocks, shared(index), None);

        let scan = store
            .scan_with_options(
                "tenant-b",
                &[],
                0,
                10,
                &ScanOptions {
                    job: Some(ScanJob {
                        object_key: "blocks/tenant-a-row-groups.parquet".into(),
                        row_group_start: 0,
                        row_group_end: 1,
                    }),
                    ..ScanOptions::default()
                },
            )
            .await
            .unwrap();
        let rows: usize = collect_table(&scan.ctx, &scan.span_table)
            .await
            .unwrap()
            .iter()
            .map(RecordBatch::num_rows)
            .sum();

        assert2::assert!(rows == 0);
    }

    #[tokio::test]
    async fn live_nested_intrinsic_values_are_returned_by_store() {
        let blocks = Arc::new(BlockStore::new(
            Arc::new(InMemory::new()),
            Url::parse("memory:///").unwrap(),
        ));
        let live = LiveTier::new(Arc::new(FakeLiveSource {
            values: vec![TypedValue {
                type_: "string".into(),
                value: "cache.miss".into(),
            }],
            frontier_ns: 0,
            ..FakeLiveSource::default()
        }));
        let store = KrabkaSpanStore::new(blocks, shared(TraceIndex::new()), Some(live));

        let values = store
            .tag_values("tenant", "event:name", 0, 10)
            .await
            .unwrap();

        assert2::assert!(
            values
                == vec![TypedValue {
                    type_: "string".into(),
                    value: "cache.miss".into(),
                }]
        );
    }

    #[tokio::test]
    async fn cold_nested_intrinsic_values_are_returned_from_trace_index() {
        let blocks = Arc::new(BlockStore::new(
            Arc::new(InMemory::new()),
            Url::parse("memory:///").unwrap(),
        ));
        let mut index = TraceIndex::new();
        index.add_trace_block(
            "tenant",
            TraceBlockStats {
                object_key: "blocks/none.parquet".into(),
                min_ts: 0,
                max_ts: 10,
                bloom: ShardedTraceBloom::new(1, 8, 0.01),
                tag_names: BTreeSet::from(["event:name".to_string()]),
                tag_values: BTreeMap::from([(
                    "event:name".to_string(),
                    BTreeSet::from(["exception".to_string()]),
                )]),
            },
        );
        let store = KrabkaSpanStore::new(blocks, shared(index), None);

        let values = store
            .tag_values("tenant", "event:name", 0, 10)
            .await
            .unwrap();

        assert2::assert!(
            values
                == vec![TypedValue {
                    type_: "string".into(),
                    value: "exception".into(),
                }]
        );
    }

    /// `cold_batches` consults a job's block before scanning it, and refuses
    /// a window it cannot serve. Its three tests survived because no case
    /// made either half of the overlap check fail on its own, and because
    /// every window used was an ordinary forward one.
    ///
    /// The block spans two spans rather than one, so `min_ts` and `max_ts`
    /// differ. With a single span they are equal, and an inverted window
    /// cannot then also overlap the block -- which is the shape that
    /// separates the early return from the overlap test doing the same job.
    #[tokio::test]
    async fn cold_batches_scans_only_a_job_whose_block_overlaps_the_window() {
        let object_store = Arc::new(InMemory::new());
        let blocks = Arc::new(BlockStore::new(
            object_store.clone(),
            Url::parse("memory:///").expect("a valid url"),
        ));
        let writer = BlockWriter::new(object_store);

        let mut early = span_with_nested_refs();
        early.start_ns = 1_000;
        let mut late = span_with_nested_refs();
        late.span_id = [3; 8];
        late.start_ns = 5_000;
        let batch = span_batch(&[early.clone(), late]).expect("the spans form a batch");
        let meta = writer
            .write_block_with_decl(
                "tenant",
                "blocks/cold.parquet",
                span_block_schema(),
                &[batch],
                &span_block_decl(),
                SummaryColumns::new(SCOL_TRACE_ID, SCOL_START_NANO),
            )
            .await
            .expect("the block writes");

        let mut index = TraceIndex::new();
        let mut bloom = ShardedTraceBloom::with_tempo_defaults(1);
        bloom.insert(&early.trace_id);
        index.add_trace_block(
            "tenant",
            TraceBlockStats {
                object_key: meta.object_key.clone(),
                min_ts: meta.min_ts,
                max_ts: meta.max_ts,
                bloom,
                tag_names: BTreeSet::new(),
                tag_values: BTreeMap::new(),
            },
        );
        let store = KrabkaSpanStore::new(blocks, shared(index), None);
        let (min, max) = (meta.min_ts, meta.max_ts);
        check!(
            min < max,
            "the block must span a range for these cases to differ"
        );

        let scan = |start: i64, end: i64, object_key: String| {
            let store = &store;
            async move {
                let job = ScanJob {
                    object_key,
                    row_group_start: 0,
                    row_group_end: 1,
                };
                store
                    .cold_batches("tenant", start, end, Some(&job))
                    .await
                    .expect("the scan runs")
                    .iter()
                    .map(RecordBatch::num_rows)
                    .sum::<usize>()
            }
        };
        let key = meta.object_key.clone();

        // The job's own block over its own range: rows come back.
        check!(scan(min, max, key.clone()).await > 0);

        // A zero-width window inside the block is legal and still scans. This
        // is the only input separating `end < start` from `end <= start`.
        check!(
            scan(max, max, key.clone()).await > 0,
            "an empty window is not an inverted one"
        );

        // An inverted window that would otherwise overlap the block. The
        // early return must catch it, and it is the overlap that makes this
        // case distinguish `<` from `==` rather than reaching the same
        // answer by a different route.
        check!(scan(max, min, key.clone()).await == 0, "end before start");

        // Windows that miss the block on one side each. Each fails exactly
        // one half of the overlap test, so loosening either `&&` shows.
        check!(
            scan(min - 100, min - 1, key.clone()).await == 0,
            "the window ends before the block starts"
        );
        check!(
            scan(max + 1, max + 100, key.clone()).await == 0,
            "the window starts after the block ends"
        );

        // A job naming a block the index does not hold scans nothing, even
        // though the window overlaps a block that IS held.
        check!(scan(min, max, "blocks/absent.parquet".to_string()).await == 0);
    }

    fn span_with_nested_refs() -> Span {
        Span {
            trace_id: [1; 16],
            span_id: [2; 8],
            parent_span_id: None,
            name: "GET /users".into(),
            kind: SpanKind::Server,
            start_ns: 1_000,
            duration_ns: 500,
            status: StatusCode::Ok,
            status_message: String::new(),
            resource_attrs: vec![KeyValue {
                key: "service.name".into(),
                value: SpanAttrValue::Str("api".into()),
            }],
            span_attrs: vec![
                KeyValue {
                    key: "http.status_code".into(),
                    value: SpanAttrValue::Int(504),
                },
                KeyValue {
                    key: "retryable".into(),
                    value: SpanAttrValue::Bool(true),
                },
            ],
            events: vec![EventRecord {
                time_unix_nano: 1_050,
                name: "exception".into(),
                attrs: vec![KeyValue {
                    key: "exception.type".into(),
                    value: SpanAttrValue::Str("timeout".into()),
                }],
            }],
            links: vec![LinkRecord {
                trace_id: [9; 16],
                span_id: [8; 8],
                attrs: vec![KeyValue {
                    key: "link.kind".into(),
                    value: SpanAttrValue::Str("retry".into()),
                }],
            }],
            instrumentation_scope: "otel-rust".into(),
            instrumentation_version: "1.2.3".into(),
        }
    }

    fn span_ref_from_span(span: &Span) -> SpanRef {
        SpanRef {
            span_id: span.span_id,
            parent_span_id: span.parent_span_id,
            name: span.name.clone(),
            kind: span.kind as i32,
            nested_set_left: 1,
            nested_set_right: 2,
            nested_set_parent: 0,
            start_time_unix_nano: u64::try_from(span.start_ns).unwrap_or_default(),
            duration: Time::from_nanos(span.duration_ns),
            status_code: span.status as i32,
            status_message: span.status_message.clone(),
            instrumentation_name: span.instrumentation_scope.clone(),
            instrumentation_version: span.instrumentation_version.clone(),
            resource_attributes: vec![],
            attributes: vec![],
            events: vec![],
            links: vec![],
        }
    }

    fn assert_cloud_region_resource_attr(attrs: &[(String, AttrValue)]) {
        assert2::assert!(
            attrs.contains(&("cloud.region".into(), AttrValue::Str("us-east-1".into())))
        );
        assert2::assert!(
            !attrs
                .iter()
                .any(|(key, _)| key == "__resource.cloud.region")
        );
    }

    #[tokio::test]
    async fn cold_trace_by_id_projects_events_and_links_from_span_blocks() {
        let object_store = Arc::new(InMemory::new());
        let blocks = Arc::new(BlockStore::new(
            object_store.clone(),
            Url::parse("memory:///").unwrap(),
        ));
        let writer = BlockWriter::new(object_store);
        let span = span_with_nested_refs();
        let batch = span_batch(std::slice::from_ref(&span)).unwrap();
        let meta = writer
            .write_block_with_decl(
                "tenant",
                "blocks/spans.parquet",
                span_block_schema(),
                &[batch],
                &span_block_decl(),
                SummaryColumns::new(SCOL_TRACE_ID, SCOL_START_NANO),
            )
            .await
            .unwrap();
        let mut bloom = ShardedTraceBloom::with_tempo_defaults(1);
        bloom.insert(&span.trace_id);
        let mut index = TraceIndex::new();
        index.add_trace_block(
            "tenant",
            TraceBlockStats {
                object_key: meta.object_key,
                min_ts: meta.min_ts,
                max_ts: meta.max_ts,
                bloom,
                tag_names: BTreeSet::new(),
                tag_values: BTreeMap::new(),
            },
        );
        let store = KrabkaSpanStore::new(blocks, shared(index), None);

        let trace = store
            .trace_by_id("tenant", &span.trace_id)
            .await
            .unwrap()
            .unwrap();

        check!(
            trace
                .spans
                .iter()
                .map(|span| (&span.attributes, &span.events, &span.links))
                .collect::<Vec<_>>()
                == vec![(
                    &vec![
                        ("http.status_code".into(), AttrValue::Int(504)),
                        ("retryable".into(), AttrValue::Bool(true)),
                    ],
                    &vec![EventRef {
                        time_since_start: nanos(50),
                        name: "exception".into(),
                        attributes: vec![(
                            "exception.type".into(),
                            AttrValue::Str("timeout".into())
                        )],
                    }],
                    &vec![LinkRef {
                        trace_id: [9; 16],
                        span_id: [8; 8],
                        attributes: vec![("link.kind".into(), AttrValue::Str("retry".into()))],
                    }],
                )]
        );
    }

    #[tokio::test]
    async fn cold_trace_by_id_within_prefilters_blocks_by_time_range() {
        let object_store = Arc::new(InMemory::new());
        let blocks = Arc::new(BlockStore::new(
            object_store.clone(),
            Url::parse("memory:///").unwrap(),
        ));
        let writer = BlockWriter::new(object_store);
        let mut early = span_with_nested_refs();
        early.span_id = [2; 8];
        early.start_ns = 1_000;
        let mut late = span_with_nested_refs();
        late.span_id = [3; 8];
        late.start_ns = 5_000;

        let early_batch = span_batch(std::slice::from_ref(&early)).unwrap();
        let early_meta = writer
            .write_block_with_decl(
                "tenant",
                "blocks/early.parquet",
                span_block_schema(),
                &[early_batch],
                &span_block_decl(),
                SummaryColumns::new(SCOL_TRACE_ID, SCOL_START_NANO),
            )
            .await
            .unwrap();
        let late_batch = span_batch(std::slice::from_ref(&late)).unwrap();
        let late_meta = writer
            .write_block_with_decl(
                "tenant",
                "blocks/late.parquet",
                span_block_schema(),
                &[late_batch],
                &span_block_decl(),
                SummaryColumns::new(SCOL_TRACE_ID, SCOL_START_NANO),
            )
            .await
            .unwrap();

        let mut index = TraceIndex::new();
        for (span, meta) in [(&early, early_meta), (&late, late_meta)] {
            let mut bloom = ShardedTraceBloom::with_tempo_defaults(1);
            bloom.insert(&span.trace_id);
            index.add_trace_block(
                "tenant",
                TraceBlockStats {
                    object_key: meta.object_key,
                    min_ts: meta.min_ts,
                    max_ts: meta.max_ts,
                    bloom,
                    tag_names: BTreeSet::new(),
                    tag_values: BTreeMap::new(),
                },
            );
        }

        let store = KrabkaSpanStore::new(blocks, shared(index), None);
        let trace = store
            .trace_by_id_within("tenant", &early.trace_id, 5_000, 5_000)
            .await
            .unwrap()
            .unwrap();

        check!(
            trace
                .spans
                .iter()
                .map(|span| span.span_id)
                .collect::<Vec<_>>()
                == vec![late.span_id]
        );
    }

    #[tokio::test]
    async fn trace_by_id_deduplicates_spans_present_in_cold_and_live_tiers() {
        let object_store = Arc::new(InMemory::new());
        let blocks = Arc::new(BlockStore::new(
            object_store.clone(),
            Url::parse("memory:///").unwrap(),
        ));
        let writer = BlockWriter::new(object_store);
        let span = span_with_nested_refs();
        let batch = span_batch(std::slice::from_ref(&span)).unwrap();
        let meta = writer
            .write_block_with_decl(
                "tenant",
                "blocks/dedup.parquet",
                span_block_schema(),
                &[batch],
                &span_block_decl(),
                SummaryColumns::new(SCOL_TRACE_ID, SCOL_START_NANO),
            )
            .await
            .unwrap();
        let mut bloom = ShardedTraceBloom::with_tempo_defaults(1);
        bloom.insert(&span.trace_id);
        let mut index = TraceIndex::new();
        index.add_trace_block(
            "tenant",
            TraceBlockStats {
                object_key: meta.object_key,
                min_ts: meta.min_ts,
                max_ts: meta.max_ts,
                bloom,
                tag_names: BTreeSet::new(),
                tag_values: BTreeMap::new(),
            },
        );
        let live = LiveTier::new(Arc::new(FakeLiveSource {
            trace: Some(TraceSpans {
                trace_id: span.trace_id,
                root_service_name: "api".into(),
                root_trace_name: "GET /users".into(),
                resource_attributes: vec![],
                spans: vec![span_ref_from_span(&span)],
            }),
            batches: vec![],
            values: vec![],
            frontier_ns: 1_000,
        }));
        let store = KrabkaSpanStore::new(blocks, shared(index), Some(live));

        let trace = store
            .trace_by_id("tenant", &span.trace_id)
            .await
            .unwrap()
            .unwrap();

        check!(
            trace
                .spans
                .iter()
                .map(|span| span.span_id)
                .collect::<Vec<_>>()
                == vec![span.span_id]
        );
    }

    #[tokio::test]
    async fn trace_by_id_recomputes_nested_sets_across_cold_and_live_tiers() {
        let object_store = Arc::new(InMemory::new());
        let blocks = Arc::new(BlockStore::new(
            object_store.clone(),
            Url::parse("memory:///").unwrap(),
        ));
        let writer = BlockWriter::new(object_store);
        let root = span_with_nested_refs();
        let mut child = span_with_nested_refs();
        child.span_id = [3; 8];
        child.parent_span_id = Some(root.span_id);
        child.start_ns = root.start_ns + 10;
        let batch = span_batch(std::slice::from_ref(&root)).unwrap();
        let meta = writer
            .write_block_with_decl(
                "tenant",
                "blocks/split-trace-root.parquet",
                span_block_schema(),
                &[batch],
                &span_block_decl(),
                SummaryColumns::new(SCOL_TRACE_ID, SCOL_START_NANO),
            )
            .await
            .unwrap();
        let mut bloom = ShardedTraceBloom::with_tempo_defaults(1);
        bloom.insert(&root.trace_id);
        let mut index = TraceIndex::new();
        index.add_trace_block(
            "tenant",
            TraceBlockStats {
                object_key: meta.object_key,
                min_ts: meta.min_ts,
                max_ts: meta.max_ts,
                bloom,
                tag_names: BTreeSet::new(),
                tag_values: BTreeMap::new(),
            },
        );
        let live = LiveTier::new(Arc::new(FakeLiveSource {
            trace: Some(TraceSpans {
                trace_id: root.trace_id,
                root_service_name: "api".into(),
                root_trace_name: "GET /users".into(),
                resource_attributes: vec![],
                spans: vec![span_ref_from_span(&child)],
            }),
            batches: vec![],
            values: vec![],
            frontier_ns: 1_000,
        }));
        let store = KrabkaSpanStore::new(blocks, shared(index), Some(live));

        let trace = store
            .trace_by_id("tenant", &root.trace_id)
            .await
            .unwrap()
            .unwrap();
        let root = trace
            .spans
            .iter()
            .find(|span| span.span_id == root.span_id)
            .unwrap();
        let child = trace
            .spans
            .iter()
            .find(|span| span.span_id == child.span_id)
            .unwrap();

        check!(child.nested_set_parent == root.nested_set_left);
        check!(child.nested_set_left > root.nested_set_left);
        check!(child.nested_set_right < root.nested_set_right);
    }

    #[tokio::test]
    async fn trace_by_id_within_keeps_spans_straddling_the_window() {
        // A by-id `start`/`end` is a candidate-selection HINT, not a hard
        // span-level filter: real Tempo returns the *whole* trace even when
        // Grafana sends a narrow window. A trace whose spans straddle the window
        // edge must return ALL its spans (so the caller can label it COMPLETE),
        // not just the spans whose start falls inside the window.
        let object_store = Arc::new(InMemory::new());
        let blocks = Arc::new(BlockStore::new(
            object_store.clone(),
            Url::parse("memory:///").unwrap(),
        ));
        let writer = BlockWriter::new(object_store);
        // Root at 1_000ns (= 0.000001s, well before the query window); child at
        // 5_000ns. Both belong to the same trace and the same block.
        let mut root = span_with_nested_refs();
        root.start_ns = 1_000;
        let mut child = span_with_nested_refs();
        child.span_id = [3; 8];
        child.parent_span_id = Some(root.span_id);
        child.start_ns = 5_000;
        let batch = span_batch(&[root.clone(), child.clone()]).unwrap();
        let meta = writer
            .write_block_with_decl(
                "tenant",
                "blocks/straddle.parquet",
                span_block_schema(),
                &[batch],
                &span_block_decl(),
                SummaryColumns::new(SCOL_TRACE_ID, SCOL_START_NANO),
            )
            .await
            .unwrap();
        let mut bloom = ShardedTraceBloom::with_tempo_defaults(1);
        bloom.insert(&root.trace_id);
        let mut index = TraceIndex::new();
        index.add_trace_block(
            "tenant",
            TraceBlockStats {
                object_key: meta.object_key,
                min_ts: meta.min_ts,
                max_ts: meta.max_ts,
                bloom,
                tag_names: BTreeSet::new(),
                tag_values: BTreeMap::new(),
            },
        );
        let store = KrabkaSpanStore::new(blocks, shared(index), None);

        // Window [4_000, 6_000] covers only the child by span start, yet the
        // block (min_ts..max_ts spans both) is still selected, so the whole
        // trace must come back — both spans, not just the child.
        let trace = store
            .trace_by_id_within("tenant", &root.trace_id, 4_000, 6_000)
            .await
            .unwrap()
            .unwrap();
        check!(
            trace
                .spans
                .iter()
                .map(|span| span.span_id)
                .collect::<Vec<_>>()
                == vec![root.span_id, child.span_id]
        );
    }

    #[tokio::test]
    async fn traceql_search_recomputes_nested_sets_across_cold_and_live_tiers() {
        let object_store = Arc::new(InMemory::new());
        let blocks = Arc::new(BlockStore::new(
            object_store.clone(),
            Url::parse("memory:///").unwrap(),
        ));
        let writer = BlockWriter::new(object_store);
        let root = span_with_nested_refs();
        let mut child = span_with_nested_refs();
        child.span_id = [3; 8];
        child.parent_span_id = Some(root.span_id);
        child.name = "db".into();
        child.start_ns = root.start_ns + 10;

        let cold_batch = span_batch(std::slice::from_ref(&root)).unwrap();
        let meta = writer
            .write_block_with_decl(
                "tenant",
                "blocks/split-trace-search-root.parquet",
                span_block_schema(),
                &[cold_batch],
                &span_block_decl(),
                SummaryColumns::new(SCOL_TRACE_ID, SCOL_START_NANO),
            )
            .await
            .unwrap();
        let mut bloom = ShardedTraceBloom::with_tempo_defaults(1);
        bloom.insert(&root.trace_id);
        let mut index = TraceIndex::new();
        index.add_trace_block(
            "tenant",
            TraceBlockStats {
                object_key: meta.object_key,
                min_ts: meta.min_ts,
                max_ts: meta.max_ts,
                bloom,
                tag_names: BTreeSet::new(),
                tag_values: BTreeMap::new(),
            },
        );
        let live = LiveTier::new(Arc::new(FakeLiveSource {
            trace: None,
            batches: vec![span_batch(std::slice::from_ref(&child)).unwrap()],
            values: vec![],
            frontier_ns: child.start_ns,
        }));
        let store = Arc::new(KrabkaSpanStore::new(blocks, shared(index), Some(live)));
        let engine = TraceqlEngine::new(store, EngineOpts::default());

        let resp = engine
            .search(
                "tenant",
                "{ span:name = \"GET /users\" } >> { span:name = \"db\" }",
                0,
                10_000,
                10,
            )
            .await
            .unwrap();

        check!(
            resp.traces
                .iter()
                .map(|trace| {
                    (
                        trace.trace_id,
                        trace
                            .span_sets
                            .iter()
                            .flat_map(|set| set.spans.iter().map(|span| span.span_id))
                            .collect::<Vec<_>>(),
                    )
                })
                .collect::<Vec<_>>()
                == vec![(root.trace_id, vec![child.span_id])]
        );
    }

    async fn event_intrinsic_fixture() -> (TraceqlEngine<KrabkaSpanStore>, [[u8; 16]; 4]) {
        let object_store = Arc::new(InMemory::new());
        let blocks = Arc::new(BlockStore::new(
            object_store.clone(),
            Url::parse("memory:///").unwrap(),
        ));
        let writer = BlockWriter::new(object_store);
        let matching = span_with_nested_refs();
        let mut other = span_with_nested_refs();
        other.trace_id = [3; 16];
        other.span_id = [4; 8];
        other.events[0].name = "cache.hit".into();
        let mut split_events = span_with_nested_refs();
        split_events.trace_id = [7; 16];
        split_events.span_id = [8; 8];
        split_events.events = vec![
            EventRecord {
                time_unix_nano: 1_050,
                name: "exception".into(),
                attrs: vec![KeyValue {
                    key: "exception.type".into(),
                    value: SpanAttrValue::Str("other".into()),
                }],
            },
            EventRecord {
                time_unix_nano: 1_060,
                name: "cache.hit".into(),
                attrs: vec![KeyValue {
                    key: "exception.type".into(),
                    value: SpanAttrValue::Str("timeout".into()),
                }],
            },
        ];
        split_events.links.push(LinkRecord {
            trace_id: [7; 16],
            span_id: [6; 8],
            attrs: Vec::new(),
        });
        let mut no_event = span_with_nested_refs();
        no_event.trace_id = [5; 16];
        no_event.span_id = [6; 8];
        no_event.events.clear();
        let batch = span_batch(&[
            matching.clone(),
            other.clone(),
            split_events.clone(),
            no_event.clone(),
        ])
        .unwrap();
        let meta = writer
            .write_block_with_decl(
                "tenant",
                "blocks/search-events.parquet",
                span_block_schema(),
                &[batch],
                &span_block_decl(),
                SummaryColumns::new(SCOL_TRACE_ID, SCOL_START_NANO),
            )
            .await
            .unwrap();
        let mut bloom = ShardedTraceBloom::with_tempo_defaults(1);
        bloom.insert(&matching.trace_id);
        bloom.insert(&other.trace_id);
        bloom.insert(&split_events.trace_id);
        bloom.insert(&no_event.trace_id);
        let mut index = TraceIndex::new();
        index.add_trace_block(
            "tenant",
            TraceBlockStats {
                object_key: meta.object_key,
                min_ts: meta.min_ts,
                max_ts: meta.max_ts,
                bloom,
                tag_names: BTreeSet::new(),
                tag_values: BTreeMap::new(),
            },
        );
        let store = Arc::new(KrabkaSpanStore::new(blocks, shared(index), None));
        let engine = TraceqlEngine::new(store, EngineOpts::default());
        (
            engine,
            [
                matching.trace_id,
                other.trace_id,
                split_events.trace_id,
                no_event.trace_id,
            ],
        )
    }

    #[tokio::test]
    async fn cold_traceql_search_filters_event_intrinsics() {
        let (engine, [matching_id, other_id, split_events_id, no_event_id]) =
            event_intrinsic_fixture().await;

        let resp = engine
            .search("tenant", "{ event:name = \"exception\" }", 0, 10_000, 10)
            .await
            .unwrap();

        check!(
            resp.traces
                .iter()
                .map(|trace| trace.trace_id)
                .collect::<BTreeSet<_>>()
                == BTreeSet::from([matching_id, split_events_id])
        );

        let resp = engine
            .search(
                "tenant",
                "{ event:name != nil } | count() by (event:name) > 1",
                0,
                10_000,
                10,
            )
            .await
            .unwrap();

        check!(
            resp.traces
                .iter()
                .map(|trace| trace.trace_id)
                .collect::<BTreeSet<_>>()
                == BTreeSet::from([matching_id, other_id, split_events_id])
        );

        let resp = engine
            .search(
                "tenant",
                "{ span:name = \"GET /users\" } | count() by (event:name) > 1",
                0,
                10_000,
                10,
            )
            .await
            .unwrap();

        check!(
            resp.traces
                .iter()
                .map(|trace| trace.trace_id)
                .collect::<BTreeSet<_>>()
                == BTreeSet::from([matching_id, other_id, split_events_id])
        );

        let resp = engine
            .search(
                "tenant",
                "{ event:name = \"exception\" && event.exception.type = \"timeout\" }",
                0,
                10_000,
                10,
            )
            .await
            .unwrap();

        assert2::assert!(resp.traces.len() == 1);
        assert2::assert!(resp.traces[0].trace_id == matching_id);

        let resp = engine
            .search("tenant", "{ event:name != nil }", 0, 10_000, 10)
            .await
            .unwrap();

        check!(resp.traces.len() == 3);
        check!(
            resp.traces
                .iter()
                .any(|trace| trace.trace_id == matching_id)
        );
        check!(resp.traces.iter().any(|trace| trace.trace_id == other_id));
        check!(
            resp.traces
                .iter()
                .any(|trace| trace.trace_id == split_events_id)
        );
        check!(
            !resp
                .traces
                .iter()
                .any(|trace| trace.trace_id == no_event_id)
        );

        let mut series = engine
            .query_range(
                "tenant",
                "{ event:name != nil } | count_over_time() | by(event:name)",
                0,
                10_000,
                10_000,
            )
            .await
            .unwrap()
            .series;

        series.sort_by(|a, b| a.labels.cmp(&b.labels));
        let cache_hit = series
            .iter()
            .find(|series| series.labels == vec![("name".into(), "cache.hit".into())])
            .unwrap();
        assert2::assert!(cache_hit.points == vec![(0, 2.0), (10_000, 0.0)]);

        let mut series = engine
            .query_range(
                "tenant",
                "{ span:name = \"GET /users\" } | count_over_time() | by(event.exception.type)",
                0,
                10_000,
                10_000,
            )
            .await
            .unwrap()
            .series;

        series.sort_by(|a, b| a.labels.cmp(&b.labels));
        // `by(event.exception.type)` groups by an event ATTRIBUTE, so the series
        // label key carries its `event.` scope (matching real Tempo, per the
        // live-Tempo differential) — unlike the bare `event:name` intrinsic above.
        assert2::assert!(series.iter().any(|series| series.labels
            == vec![("event.exception.type".into(), "timeout".into())]
            && series.points == vec![(0, 3.0), (10_000, 0.0)]));

        let mut series = engine
            .query_range(
                "tenant",
                "{ span:name = \"GET /users\" } | count_over_time() | by(link:spanID)",
                0,
                10_000,
                10_000,
            )
            .await
            .unwrap()
            .series;

        series.sort_by(|a, b| a.labels.cmp(&b.labels));
        assert2::assert!(series.iter().any(|series| series.labels
            == vec![("spanID".into(), "0606060606060606".into())]
            && series.points == vec![(0, 1.0), (10_000, 0.0)]));
    }

    #[tokio::test]
    async fn cold_traceql_search_applies_repeated_attr_any_none_semantics() {
        let object_store = Arc::new(InMemory::new());
        let blocks = Arc::new(BlockStore::new(
            object_store.clone(),
            Url::parse("memory:///").unwrap(),
        ));
        let writer = BlockWriter::new(object_store);
        let mut repeated = span_with_nested_refs();
        repeated.span_attrs.push(KeyValue {
            key: "http.method".into(),
            value: SpanAttrValue::Str("GET".into()),
        });
        repeated.span_attrs.push(KeyValue {
            key: "http.method".into(),
            value: SpanAttrValue::Str("POST".into()),
        });
        let mut other = span_with_nested_refs();
        other.trace_id = [3; 16];
        other.span_id = [4; 8];
        other.span_attrs.push(KeyValue {
            key: "http.method".into(),
            value: SpanAttrValue::Str("DELETE".into()),
        });
        let batch = span_batch(&[repeated.clone(), other.clone()]).unwrap();
        let meta = writer
            .write_block_with_decl(
                "tenant",
                "blocks/search-array-attrs.parquet",
                span_block_schema(),
                &[batch],
                &span_block_decl(),
                SummaryColumns::new(SCOL_TRACE_ID, SCOL_START_NANO),
            )
            .await
            .unwrap();
        let mut bloom = ShardedTraceBloom::with_tempo_defaults(1);
        bloom.insert(&repeated.trace_id);
        bloom.insert(&other.trace_id);
        let mut index = TraceIndex::new();
        index.add_trace_block(
            "tenant",
            TraceBlockStats {
                object_key: meta.object_key,
                min_ts: meta.min_ts,
                max_ts: meta.max_ts,
                bloom,
                tag_names: BTreeSet::new(),
                tag_values: BTreeMap::new(),
            },
        );
        let store = Arc::new(KrabkaSpanStore::new(blocks, shared(index), None));
        let engine = TraceqlEngine::new(store, EngineOpts::default());

        let resp = engine
            .search("tenant", "{ span.http.method = \"POST\" }", 0, 10_000, 10)
            .await
            .unwrap();
        assert2::assert!(resp.traces.len() == 1);
        assert2::assert!(resp.traces[0].trace_id == repeated.trace_id);

        let resp = engine
            .search("tenant", "{ span.http.method != \"POST\" }", 0, 10_000, 10)
            .await
            .unwrap();
        assert2::assert!(resp.traces.len() == 1);
        assert2::assert!(resp.traces[0].trace_id == other.trace_id);
    }

    #[tokio::test]
    async fn cold_traceql_search_keeps_resource_and_span_scopes_distinct() {
        let object_store = Arc::new(InMemory::new());
        let blocks = Arc::new(BlockStore::new(
            object_store.clone(),
            Url::parse("memory:///").unwrap(),
        ));
        let writer = BlockWriter::new(object_store);
        let mut span = span_with_nested_refs();
        span.resource_attrs.push(KeyValue {
            key: "cloud.region".into(),
            value: SpanAttrValue::Str("us-east-1".into()),
        });
        let batch = span_batch(std::slice::from_ref(&span)).unwrap();
        let meta = writer
            .write_block_with_decl(
                "tenant",
                "blocks/search-resource-scope.parquet",
                span_block_schema(),
                &[batch],
                &span_block_decl(),
                SummaryColumns::new(SCOL_TRACE_ID, SCOL_START_NANO),
            )
            .await
            .unwrap();
        let mut bloom = ShardedTraceBloom::with_tempo_defaults(1);
        bloom.insert(&span.trace_id);
        let mut index = TraceIndex::new();
        index.add_trace_block(
            "tenant",
            TraceBlockStats {
                object_key: meta.object_key,
                min_ts: meta.min_ts,
                max_ts: meta.max_ts,
                bloom,
                tag_names: BTreeSet::new(),
                tag_values: BTreeMap::new(),
            },
        );
        let store = Arc::new(KrabkaSpanStore::new(blocks, shared(index), None));
        let engine = TraceqlEngine::new(store, EngineOpts::default());

        let resource = engine
            .search(
                "tenant",
                "{ resource.service.name = \"api\" }",
                0,
                10_000,
                10,
            )
            .await
            .unwrap();
        assert2::assert!(resource.traces.len() == 1);

        let bare = engine
            .search("tenant", "{ .service.name = \"api\" }", 0, 10_000, 10)
            .await
            .unwrap();
        assert2::assert!(bare.traces.len() == 1);

        let resource_attr = engine
            .search(
                "tenant",
                "{ resource.cloud.region = \"us-east-1\" }",
                0,
                10_000,
                10,
            )
            .await
            .unwrap();
        assert2::assert!(resource_attr.traces.len() == 1);

        let trace = engine
            .trace_by_id("tenant", &span.trace_id)
            .await
            .unwrap()
            .unwrap();
        assert_cloud_region_resource_attr(&trace.resource_attributes);
        assert_cloud_region_resource_attr(&trace.spans[0].resource_attributes);

        let bare_attr = engine
            .search("tenant", "{ .cloud.region = \"us-east-1\" }", 0, 10_000, 10)
            .await
            .unwrap();
        assert2::assert!(bare_attr.traces.len() == 1);

        let span = engine
            .search("tenant", "{ span.service.name = \"api\" }", 0, 10_000, 10)
            .await
            .unwrap();
        assert2::assert!(span.traces.is_empty());
    }

    #[tokio::test]
    async fn cold_traceql_metrics_group_resource_service_name_after_nil_guard() {
        let object_store = Arc::new(InMemory::new());
        let blocks = Arc::new(BlockStore::new(
            object_store.clone(),
            Url::parse("memory:///").unwrap(),
        ));
        let writer = BlockWriter::new(object_store);
        let mut checkout = span_with_nested_refs();
        checkout.trace_id = [1; 16];
        checkout.span_id = [1; 8];
        checkout.start_ns = 1_000;
        checkout.resource_attrs = vec![KeyValue {
            key: "service.name".into(),
            value: SpanAttrValue::Str("checkout".into()),
        }];
        let mut billing = span_with_nested_refs();
        billing.trace_id = [2; 16];
        billing.span_id = [2; 8];
        billing.start_ns = 2_000;
        billing.resource_attrs = vec![KeyValue {
            key: "service.name".into(),
            value: SpanAttrValue::Str("billing".into()),
        }];
        let meta = writer
            .write_block_with_decl(
                "tenant",
                "blocks/metrics-resource-service-name.parquet",
                span_block_schema(),
                &[
                    span_batch(std::slice::from_ref(&checkout)).unwrap(),
                    span_batch(std::slice::from_ref(&billing)).unwrap(),
                ],
                &span_block_decl(),
                SummaryColumns::new(SCOL_TRACE_ID, SCOL_START_NANO),
            )
            .await
            .unwrap();
        let mut bloom = ShardedTraceBloom::with_tempo_defaults(1);
        bloom.insert(&checkout.trace_id);
        bloom.insert(&billing.trace_id);
        let mut index = TraceIndex::new();
        index.add_trace_block(
            "tenant",
            TraceBlockStats {
                object_key: meta.object_key,
                min_ts: meta.min_ts,
                max_ts: meta.max_ts,
                bloom,
                tag_names: BTreeSet::from(["service.name".to_string()]),
                tag_values: BTreeMap::from([(
                    "service.name".to_string(),
                    BTreeSet::from(["billing".to_string(), "checkout".to_string()]),
                )]),
            },
        );
        let store = Arc::new(KrabkaSpanStore::new(blocks, shared(index), None));
        let engine = TraceqlEngine::new(store, EngineOpts::default());

        let mut series = engine
            .query_range(
                "tenant",
                "{ resource.service.name != nil } | count_over_time() by(resource.service.name)",
                0,
                10_000,
                10_000,
            )
            .await
            .unwrap()
            .series;

        series.sort_by(|a, b| a.labels.cmp(&b.labels));
        check!(
            series
                .iter()
                .map(|series| (series.labels.clone(), series.points.clone()))
                .collect::<Vec<_>>()
                == ["billing", "checkout"]
                    .into_iter()
                    .map(|service| {
                        (
                            vec![("resource.service.name".into(), service.into())],
                            vec![(0, 1.0), (10_000, 0.0)],
                        )
                    })
                    .collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn cold_traceql_search_applies_block_array_attr_any_none_semantics() {
        let object_store = Arc::new(InMemory::new());
        let blocks = Arc::new(BlockStore::new(
            object_store.clone(),
            Url::parse("memory:///").unwrap(),
        ));
        let writer = BlockWriter::new(object_store);
        let rows = vec![
            block_attr_span_row(
                [1; 16],
                [2; 8],
                "GET /users",
                true,
                vec!["GET".into(), "POST".into()],
            ),
            block_attr_span_row(
                [3; 16],
                [4; 8],
                "DELETE /users",
                false,
                vec!["DELETE".into()],
            ),
        ];
        let batch = encode_span_rows(&rows).unwrap();
        let meta = writer
            .write_block_with_decl(
                "tenant",
                "blocks/search-block-array-attrs.parquet",
                span_block_schema(),
                &[batch],
                &span_block_decl(),
                SummaryColumns::new(SCOL_TRACE_ID, SCOL_START_NANO),
            )
            .await
            .unwrap();
        let mut bloom = ShardedTraceBloom::with_tempo_defaults(1);
        bloom.insert(&[1; 16]);
        bloom.insert(&[3; 16]);
        let mut index = TraceIndex::new();
        index.add_trace_block(
            "tenant",
            TraceBlockStats {
                object_key: meta.object_key,
                min_ts: meta.min_ts,
                max_ts: meta.max_ts,
                bloom,
                tag_names: BTreeSet::new(),
                tag_values: BTreeMap::new(),
            },
        );
        let store = Arc::new(KrabkaSpanStore::new(blocks, shared(index), None));
        let engine = TraceqlEngine::new(store, EngineOpts::default());

        let resp = engine
            .search("tenant", "{ span.http.method = \"POST\" }", 0, 10_000, 10)
            .await
            .unwrap();
        check!(
            resp.traces
                .iter()
                .map(|trace| {
                    (
                        trace.trace_id,
                        trace.span_sets[0].spans[0].attributes.clone(),
                    )
                })
                .collect::<Vec<_>>()
                == vec![(
                    [1; 16],
                    vec![
                        ("http.method".into(), AttrValue::Str("GET".into())),
                        ("http.method".into(), AttrValue::Str("POST".into())),
                    ],
                )]
        );

        let resp = engine
            .search("tenant", "{ span.http.method != \"POST\" }", 0, 10_000, 10)
            .await
            .unwrap();
        assert2::assert!(resp.traces.len() == 1);
        assert2::assert!(resp.traces[0].trace_id == [3; 16]);
    }

    fn block_attr_span_row(
        trace_id: [u8; 16],
        span_id: [u8; 8],
        name: &str,
        is_array: bool,
        values: Vec<String>,
    ) -> SpanRow {
        SpanRow {
            trace_id,
            span_id,
            parent_span_id: None,
            nested_set: BlockNestedSet {
                nested_set_left: 1,
                nested_set_right: 2,
                parent_id: 0,
            },
            child_count: 0,
            root_service_name: Some("api".into()),
            root_span_name: Some("root".into()),
            trace_start_unix_nano: 1_000,
            trace_duration: nanos(500),
            name: Some(name.into()),
            kind: BlockSpanKind::Server,
            start_unix_nano: 1_000,
            duration: nanos(500),
            status_code: BlockStatusCode::Ok,
            status_message: None,
            instrumentation_name: Some("otel-rust".into()),
            instrumentation_version: None,
            attrs: vec![SpanAttr {
                key: "http.method".into(),
                is_array,
                value: BlockAttrValue::Str(values),
            }],
            events: Vec::new(),
            links: Vec::new(),
        }
    }

    #[tokio::test]
    async fn can_back_traceql_engine() {
        let blocks = Arc::new(BlockStore::new(
            Arc::new(InMemory::new()),
            Url::parse("memory:///").unwrap(),
        ));
        let store = Arc::new(KrabkaSpanStore::new(
            blocks,
            shared(TraceIndex::new()),
            None,
        ));
        let engine = TraceqlEngine::new(store, EngineOpts::default());
        let resp = engine
            .search("tenant", "{ span:name = \"missing\" }", 0, 10, 10)
            .await
            .unwrap();
        assert2::assert!(resp.traces.is_empty());
    }

    /// Verify that a reader observes a live `ArcSwap`.
    ///
    /// `candidate_blocks` returns nothing from the initial empty index. After a
    /// `store()` on the shared handle, the new block is immediately visible,
    /// both directly and through the `KrabkaSpanStore` that holds the same
    /// `Arc<ArcSwap<TraceIndex>>`.
    #[tokio::test]
    async fn span_store_observes_swapped_index() {
        let blocks = Arc::new(BlockStore::new(
            Arc::new(InMemory::new()),
            Url::parse("memory:///").unwrap(),
        ));
        let handle: SharedTraceIndex = shared(TraceIndex::new());
        // Build the store — it holds the same Arc so it observes every swap.
        let _store = KrabkaSpanStore::new(Arc::clone(&blocks), Arc::clone(&handle), None);

        // Before swap: no candidate blocks.
        let before = handle.load().candidate_blocks("tenant", 0, i64::MAX);
        assert2::assert!(before.is_empty());

        // Swap in an index with one block.
        let mut new_index = TraceIndex::new();
        let mut bloom = ShardedTraceBloom::with_tempo_defaults(1);
        bloom.insert(&[1_u8; 16]);
        new_index.add_trace_block(
            "tenant",
            TraceBlockStats {
                object_key: "blocks/swap-test.parquet".into(),
                min_ts: 0,
                max_ts: 10_000,
                bloom,
                tag_names: BTreeSet::new(),
                tag_values: BTreeMap::new(),
            },
        );
        handle.store(Arc::new(new_index));

        // After swap: candidate_blocks via the same handle now returns the new block.
        let after = handle.load().candidate_blocks("tenant", 0, 10_000);
        assert2::assert!(!after.is_empty());
        assert2::assert!(after.first().map(String::as_str) == Some("blocks/swap-test.parquet"));

        // Any subsequent load() call through the store's field would return
        // the same result — both the store and the caller share the same Arc.
        let via_handle = handle.load().candidate_blocks("tenant", 0, 10_000);
        assert2::assert!(via_handle == after);
    }
}

mod add_nested_intrinsic_columns;
mod add_nested_intrinsic_columns_to_batch;
mod add_span_attr_columns;
mod add_span_attr_columns_to_batch;
mod align_scan_batch_to_schema;
mod align_scan_batches_to_schema;
mod append_nested_attr;
mod append_nested_event;
mod append_nested_link;
mod attr_matches;
mod attr_typed_value_parts;
mod attr_value_label;
mod attr_values;
mod attr_values_match;
mod attr_values_with_resource;
mod batch_attr_matches;
mod batch_attr_matches_with_resource;
mod block_attr_values;
mod block_attr_values_for_key;
mod block_err;
mod bool_array_value;
mod bool_attr_values;
mod bool_matches;
mod bytes_to_hex;
mod cold_attribute_tag_names;
mod collect_attribute_tag_names;
mod collect_attribute_tag_values;
mod collect_intrinsic_value;
mod collect_table;
mod deduplicate_trace_spans;
mod default_scan_concat_max;
mod enum_int_matches;
mod event_matcher_matches_absence;
mod event_matcher_matches_event;
mod event_tags;
mod event_values;
mod f64_attr_values;
mod filter_batches_by_matchers;
mod fixed;
mod fixed_array_value;
mod fixed_value;
mod float64_array_value;
mod float_matches;
mod i64_attr_values;
mod insert_i32_value;
mod insert_i64_value;
mod insert_string_value;
mod instrumentation_matches;
mod instrumentation_tags;
mod int32_value;
mod int64_array_value;
mod int64_value;
mod int_matches;
mod intrinsic_matches;
mod intrinsic_tags;
mod intrinsic_values_from_batches;
mod is_event_matcher;
mod is_intrinsic_tag;
mod is_link_matcher;
mod is_nested_intrinsic_tag;
mod kind_enum_value;
mod krabka_span_store;
mod link_matcher_matches_absence;
mod link_matcher_matches_link;
mod link_tags;
mod link_values;
mod matching_events_for_scan;
mod matching_links_for_scan;
mod merge_dynamic_scope;
mod merge_static_scope;
mod nested_attr_column;
mod nested_attr_columns;
mod nested_attr_scope;
mod nested_event_matchers_match;
mod nested_intrinsic_rows;
mod nested_link_matchers_match;
mod nested_presence_matches;
mod nested_string_attrs;
mod nil_matches;
mod nullable_fixed_value;
mod optional_list_column;
mod present_value_matches;
mod recompute_batch_nested_sets;
mod recompute_scan_nested_sets;
mod recompute_trace_nested_sets;
mod replace_scan_int32_columns;
mod resource_attr_values;
mod resource_matches;
mod root_service_matches;
mod row_attr_values;
mod row_matcher_matches;
mod row_matches;
mod scope_order;
mod shared_trace_index;
mod status_enum_value;
mod string_array_value;
mod string_attr_values;
mod string_matches;
mod string_value;
mod struct_fixed_field;
mod struct_int64_field;
mod struct_list_field;
mod struct_string_field;
mod tag_scope_key;
mod trace_from_batches;
mod unscoped_attribute_tag;

use add_nested_intrinsic_columns::add_nested_intrinsic_columns;
use add_nested_intrinsic_columns_to_batch::add_nested_intrinsic_columns_to_batch;
use add_span_attr_columns::add_span_attr_columns;
use add_span_attr_columns_to_batch::add_span_attr_columns_to_batch;
use align_scan_batch_to_schema::align_scan_batch_to_schema;
use align_scan_batches_to_schema::align_scan_batches_to_schema;
use append_nested_attr::append_nested_attr;
use append_nested_event::append_nested_event;
use append_nested_link::append_nested_link;
use attr_matches::attr_matches;
use attr_typed_value_parts::attr_typed_value_parts;
use attr_value_label::attr_value_label;
use attr_values::attr_values;
use attr_values_match::attr_values_match;
use attr_values_with_resource::attr_values_with_resource;
use batch_attr_matches::batch_attr_matches;
use batch_attr_matches_with_resource::batch_attr_matches_with_resource;
use block_attr_values::block_attr_values;
use block_attr_values_for_key::block_attr_values_for_key;
use block_err::block_err;
use bool_array_value::bool_array_value;
use bool_attr_values::bool_attr_values;
use bool_matches::bool_matches;
use bytes_to_hex::bytes_to_hex;
use cold_attribute_tag_names::ColdAttributeTagNames;
use collect_attribute_tag_names::collect_attribute_tag_names;
use collect_attribute_tag_values::collect_attribute_tag_values;
use collect_intrinsic_value::collect_intrinsic_value;
use collect_table::collect_table;
use deduplicate_trace_spans::deduplicate_trace_spans;
pub use default_scan_concat_max::DEFAULT_SCAN_CONCAT_MAX;
use enum_int_matches::enum_int_matches;
use event_matcher_matches_absence::event_matcher_matches_absence;
use event_matcher_matches_event::event_matcher_matches_event;
use event_tags::EVENT_TAGS;
use event_values::event_values;
use f64_attr_values::f64_attr_values;
use filter_batches_by_matchers::filter_batches_by_matchers;
use fixed::fixed;
use fixed_array_value::fixed_array_value;
use fixed_value::fixed_value;
use float_matches::float_matches;
use float64_array_value::float64_array_value;
use i64_attr_values::i64_attr_values;
use insert_i32_value::insert_i32_value;
use insert_i64_value::insert_i64_value;
use insert_string_value::insert_string_value;
use instrumentation_matches::instrumentation_matches;
use instrumentation_tags::INSTRUMENTATION_TAGS;
use int_matches::int_matches;
use int32_value::int32_value;
use int64_array_value::int64_array_value;
use int64_value::int64_value;
use intrinsic_matches::intrinsic_matches;
use intrinsic_tags::INTRINSIC_TAGS;
use intrinsic_values_from_batches::intrinsic_values_from_batches;
use is_event_matcher::is_event_matcher;
use is_intrinsic_tag::is_intrinsic_tag;
use is_link_matcher::is_link_matcher;
use is_nested_intrinsic_tag::is_nested_intrinsic_tag;
use kind_enum_value::kind_enum_value;
pub use krabka_span_store::KrabkaSpanStore;
use link_matcher_matches_absence::link_matcher_matches_absence;
use link_matcher_matches_link::link_matcher_matches_link;
use link_tags::LINK_TAGS;
use link_values::link_values;
use matching_events_for_scan::matching_events_for_scan;
use matching_links_for_scan::matching_links_for_scan;
use merge_dynamic_scope::merge_dynamic_scope;
use merge_static_scope::merge_static_scope;
use nested_attr_column::NestedAttrColumn;
use nested_attr_columns::nested_attr_columns;
use nested_attr_scope::NestedAttrScope;
use nested_event_matchers_match::nested_event_matchers_match;
use nested_intrinsic_rows::nested_intrinsic_rows;
use nested_link_matchers_match::nested_link_matchers_match;
use nested_presence_matches::nested_presence_matches;
use nested_string_attrs::nested_string_attrs;
use nil_matches::nil_matches;
use nullable_fixed_value::nullable_fixed_value;
use optional_list_column::optional_list_column;
use present_value_matches::present_value_matches;
use recompute_batch_nested_sets::recompute_batch_nested_sets;
use recompute_scan_nested_sets::recompute_scan_nested_sets;
use recompute_trace_nested_sets::recompute_trace_nested_sets;
use replace_scan_int32_columns::replace_scan_int32_columns;
use resource_attr_values::resource_attr_values;
use resource_matches::resource_matches;
use root_service_matches::root_service_matches;
use row_attr_values::row_attr_values;
use row_matcher_matches::row_matcher_matches;
use row_matches::row_matches;
use scope_order::SCOPE_ORDER;
pub use shared_trace_index::SharedTraceIndex;
use status_enum_value::status_enum_value;
use string_array_value::string_array_value;
use string_attr_values::string_attr_values;
use string_matches::string_matches;
use string_value::string_value;
use struct_fixed_field::struct_fixed_field;
use struct_int64_field::struct_int64_field;
use struct_list_field::struct_list_field;
use struct_string_field::struct_string_field;
use tag_scope_key::tag_scope_key;
use trace_from_batches::trace_from_batches;
use unscoped_attribute_tag::unscoped_attribute_tag;
