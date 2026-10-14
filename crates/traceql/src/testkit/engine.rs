use super::{Arc, AttrValue, Attribute, EngineOpts, InMemorySpanStore, SpanFixture, TraceqlEngine};

/// An engine over the fixture traces the golden corpus and the golden query
/// suite assert against: tenant `t`, three traces of one to four spans.
#[must_use]
pub fn engine() -> TraceqlEngine<InMemorySpanStore> {
    let mut store = InMemorySpanStore::new();
    store.push_trace(
        "t",
        "svc-a",
        "root-a",
        vec![
            SpanFixture {
                trace: 1,
                id: 1,
                name: "root-a",
                duration_nanos: 100,
                attrs: vec![
                    Attribute::new("svc", AttrValue::Str("a".into())),
                    Attribute::new("a", AttrValue::Int(1)),
                    Attribute::new("http.method", AttrValue::Str("GET".into())),
                    Attribute::new("name", AttrValue::Str("post-root".into())),
                ],
                ..SpanFixture::default()
            }
            .input_span(),
            SpanFixture {
                trace: 1,
                id: 2,
                parent: Some(1),
                name: "child-x",
                duration_nanos: 200,
                attrs: vec![
                    Attribute::new("svc", AttrValue::Str("b".into())),
                    Attribute::new("b", AttrValue::Int(2)),
                ],
            }
            .input_span(),
            SpanFixture {
                trace: 1,
                id: 4,
                parent: Some(2),
                name: "grand-y",
                duration_nanos: 80,
                attrs: vec![Attribute::new("svc", AttrValue::Str("c".into()))],
            }
            .input_span(),
            SpanFixture {
                trace: 1,
                id: 3,
                parent: Some(1),
                name: "child-z",
                duration_nanos: 220,
                attrs: vec![Attribute::new("svc", AttrValue::Str("b".into()))],
            }
            .input_span(),
        ],
    );
    store.push_trace(
        "t",
        "svc-x",
        "root-x",
        vec![
            SpanFixture {
                trace: 2,
                id: 1,
                name: "both",
                duration_nanos: 50,
                attrs: vec![
                    Attribute::new("svc", AttrValue::Str("x".into())),
                    Attribute::new("a", AttrValue::Int(1)),
                    Attribute::new("b", AttrValue::Int(2)),
                    Attribute::new("name", AttrValue::Str("xpost".into())),
                ],
                ..SpanFixture::default()
            }
            .input_span(),
        ],
    );
    store.push_trace(
        "t",
        "svc-d",
        "root-d",
        vec![
            SpanFixture {
                trace: 3,
                id: 1,
                name: "root-d",
                duration_nanos: 100,
                attrs: vec![Attribute::new("svc", AttrValue::Str("a".into()))],
                ..SpanFixture::default()
            }
            .input_span(),
            SpanFixture {
                trace: 3,
                id: 2,
                parent: Some(1),
                name: "child-d",
                duration_nanos: 100,
                attrs: vec![Attribute::new("svc", AttrValue::Str("d".into()))],
            }
            .input_span(),
        ],
    );
    TraceqlEngine::new(Arc::new(store), EngineOpts::default())
}
