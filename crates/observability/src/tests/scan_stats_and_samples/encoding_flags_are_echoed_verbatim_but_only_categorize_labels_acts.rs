use super::*;

/// Loki reads `X-Loki-Response-Encoding-Flags` as a comma-separated list that
/// it neither trims nor lowercases, echoes back exactly as given -- a flag it
/// does not know included -- and matches against `categorize-labels` exactly.
/// A repeated header counts once, from its first line.
///
/// The echo reaches a `streams` answer only. A metric query that carried the
/// header comes back without `encodingFlags`.
#[test]
pub(crate) fn encoding_flags_are_echoed_verbatim_but_only_categorize_labels_acts() {
    let headers = |values: &[&str]| {
        let mut headers = HeaderMap::new();
        for value in values {
            headers.append(
                "X-Loki-Response-Encoding-Flags",
                value.parse().expect("a header value"),
            );
        }
        headers
    };

    for (values, flags, encoding) in [
        (&[][..], &[][..], LokiStreamEncoding::Folded),
        (&[""], &[], LokiStreamEncoding::Folded),
        (
            &["categorize-labels"],
            &["categorize-labels"],
            LokiStreamEncoding::CategorizeLabels,
        ),
        (
            &["CATEGORIZE-LABELS"],
            &["CATEGORIZE-LABELS"],
            LokiStreamEncoding::Folded,
        ),
        (&["bogus"], &["bogus"], LokiStreamEncoding::Folded),
        (
            &["categorize-labels, other"],
            &["categorize-labels", " other"],
            LokiStreamEncoding::CategorizeLabels,
        ),
        (
            &["other", "categorize-labels"],
            &["other"],
            LokiStreamEncoding::Folded,
        ),
    ] {
        let headers = headers(values);
        let expected = flags
            .iter()
            .map(|flag| (*flag).to_string())
            .collect::<Vec<_>>();
        check!(loki_encoding_flags(&headers) == expected, "{values:?}");
        check!(
            loki_stream_encoding_for_headers(&headers) == encoding,
            "{values:?}"
        );
    }

    let flags = vec!["categorize-labels".to_string()];
    let mut streams = json!({"status": "success", "data": {"resultType": "streams", "result": []}});
    add_loki_encoding_flags(&mut streams, &flags);
    check!(
        streams
            == json!({
                "status": "success",
                "data": {
                    "resultType": "streams",
                    "result": [],
                    "encodingFlags": ["categorize-labels"]
                }
            })
    );

    let mut matrix = json!({"status": "success", "data": {"resultType": "matrix", "result": []}});
    add_loki_encoding_flags(&mut matrix, &flags);
    check!(
        matrix == json!({"status": "success", "data": {"resultType": "matrix", "result": []}}),
        "only a streams answer reports the flags"
    );

    let mut frame = json!({"streams": []});
    add_loki_tail_encoding_flags(&mut frame, &flags);
    check!(frame == json!({"streams": [], "encodingFlags": ["categorize-labels"]}));
}
