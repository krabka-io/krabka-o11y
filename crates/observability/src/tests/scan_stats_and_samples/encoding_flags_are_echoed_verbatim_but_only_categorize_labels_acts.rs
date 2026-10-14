use super::*;

struct EncodingFlagCase {
    values: &'static [&'static str],
    flags: &'static [&'static str],
    encoding: LokiStreamEncoding,
}

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

    for case in [
        EncodingFlagCase {
            values: &[],
            flags: &[],
            encoding: LokiStreamEncoding::Folded,
        },
        EncodingFlagCase {
            values: &[""],
            flags: &[],
            encoding: LokiStreamEncoding::Folded,
        },
        EncodingFlagCase {
            values: &["categorize-labels"],
            flags: &["categorize-labels"],
            encoding: LokiStreamEncoding::CategorizeLabels,
        },
        EncodingFlagCase {
            values: &["CATEGORIZE-LABELS"],
            flags: &["CATEGORIZE-LABELS"],
            encoding: LokiStreamEncoding::Folded,
        },
        EncodingFlagCase {
            values: &["bogus"],
            flags: &["bogus"],
            encoding: LokiStreamEncoding::Folded,
        },
        EncodingFlagCase {
            values: &["categorize-labels, other"],
            flags: &["categorize-labels", " other"],
            encoding: LokiStreamEncoding::CategorizeLabels,
        },
        EncodingFlagCase {
            values: &["other", "categorize-labels"],
            flags: &["other"],
            encoding: LokiStreamEncoding::Folded,
        },
    ] {
        let request_headers = headers(case.values);
        let expected = case
            .flags
            .iter()
            .map(|flag| (*flag).to_string())
            .collect::<Vec<_>>();
        check!(
            loki_encoding_flags(&request_headers) == expected,
            "{:?}",
            case.values
        );
        check!(
            loki_stream_encoding_for_headers(&request_headers) == case.encoding,
            "{:?}",
            case.values
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
    // Where the flags sit, not only that they are there. A `Value` comparison
    // cannot see this: `serde_json`'s map compares without regard to order,
    // while the bytes it writes keep it. Grafana's Loki datasource reads the
    // body in one pass and needs `encodingFlags` before `result`, because that
    // flag is what tells it an entry is three elements long. Written after
    // `result`, the flag arrives too late and the datasource fails the query.
    let body = streams.to_string();
    check!(
        body.find("encodingFlags") < body.find("\"result\""),
        "the flags belong in front of the result: {body}"
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
