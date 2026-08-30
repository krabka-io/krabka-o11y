use super::*;

pub(crate) fn engine() -> TraceqlEngine<InMemorySpanStore> {
    let mut store = InMemorySpanStore::new();
    store.push_trace(
        "t",
        "svc-a",
        "root-a",
        vec![
            span(
                1,
                1,
                None,
                "root-a",
                100,
                vec![
                    ("svc", AttrValue::Str("a".into())),
                    ("a", AttrValue::Int(1)),
                    ("http.method", AttrValue::Str("GET".into())),
                    ("name", AttrValue::Str("post-root".into())),
                ],
            ),
            span(
                1,
                2,
                Some(1),
                "child-x",
                200,
                vec![
                    ("svc", AttrValue::Str("b".into())),
                    ("b", AttrValue::Int(2)),
                ],
            ),
            span(
                1,
                4,
                Some(2),
                "grand-y",
                80,
                vec![("svc", AttrValue::Str("c".into()))],
            ),
            span(
                1,
                3,
                Some(1),
                "child-z",
                220,
                vec![("svc", AttrValue::Str("b".into()))],
            ),
        ],
    );
    store.push_trace(
        "t",
        "svc-x",
        "root-x",
        vec![span(
            2,
            1,
            None,
            "both",
            50,
            vec![
                ("svc", AttrValue::Str("x".into())),
                ("a", AttrValue::Int(1)),
                ("b", AttrValue::Int(2)),
                ("name", AttrValue::Str("xpost".into())),
            ],
        )],
    );
    store.push_trace(
        "t",
        "svc-d",
        "root-d",
        vec![
            span(
                3,
                1,
                None,
                "root-d",
                100,
                vec![("svc", AttrValue::Str("a".into()))],
            ),
            span(
                3,
                2,
                Some(1),
                "child-d",
                100,
                vec![("svc", AttrValue::Str("d".into()))],
            ),
        ],
    );
    TraceqlEngine::new(Arc::new(store), EngineOpts::default())
}
