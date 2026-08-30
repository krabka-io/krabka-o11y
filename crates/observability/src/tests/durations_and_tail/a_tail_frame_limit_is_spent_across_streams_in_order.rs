use super::*;

/// `apply_loki_tail_frame_limit` spends one budget across a frame's
/// streams, and `tail_frame_is_empty` decides whether the result is worth
/// sending at all. The two work together: the limiter drops streams it
/// empties, so a frame limited down to nothing has no streams left and
/// the emptiness check -- which reads the streams array, not the values
/// inside it -- then suppresses the frame.
#[test]
pub(crate) fn a_tail_frame_limit_is_spent_across_streams_in_order() {
    let frame = |counts: &[usize]| {
        serde_json::json!({
            "streams": counts
                .iter()
                .map(|count| serde_json::json!({
                    "stream": {"app": "api"},
                    "values": (0..*count)
                        .map(|i| serde_json::json!([i.to_string(), "line"]))
                        .collect::<Vec<_>>(),
                }))
                .collect::<Vec<_>>(),
        })
    };
    let kept = |value: &serde_json::Value| {
        value["streams"]
            .as_array()
            .expect("streams is an array")
            .iter()
            .map(|stream| stream["values"].as_array().map_or(0, Vec::len))
            .collect::<Vec<_>>()
    };

    // The first stream takes 2 of the 5 and the second takes the rest.
    check!(
        kept(&super::super::prelude::apply_loki_tail_frame_limit(
            frame(&[2, 10]),
            Some(5)
        )) == vec![2, 3]
    );
    // A stream that exhausts the budget leaves nothing for the later ones,
    // and emptied streams are dropped rather than sent with no values --
    // the same rule as the search path.
    check!(
        kept(&super::super::prelude::apply_loki_tail_frame_limit(
            frame(&[5, 10]),
            Some(5)
        )) == vec![5]
    );
    check!(
        kept(&super::super::prelude::apply_loki_tail_frame_limit(
            frame(&[2, 2]),
            Some(5)
        )) == vec![2, 2]
    );
    check!(
        kept(&super::super::prelude::apply_loki_tail_frame_limit(
            frame(&[9]),
            None
        )) == vec![9]
    );
    check!(
        kept(&super::super::prelude::apply_loki_tail_frame_limit(
            frame(&[9]),
            Some(0)
        ))
        .is_empty(),
        "a zero limit empties every stream, and empty streams are dropped"
    );

    // Emptiness is about the streams array, not the values in it.
    check!(super::super::prelude::tail_frame_is_empty(&frame(&[])));
    check!(super::super::prelude::tail_frame_is_empty(
        &serde_json::json!({})
    ));
    check!(
        !super::super::prelude::tail_frame_is_empty(&frame(&[0])),
        "a stream carrying no values is still a stream"
    );
    check!(!super::super::prelude::tail_frame_is_empty(&frame(&[1])));
}
