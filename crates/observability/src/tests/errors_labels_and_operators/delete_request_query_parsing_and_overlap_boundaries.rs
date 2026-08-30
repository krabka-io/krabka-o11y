use super::*;

#[test]
pub(crate) fn delete_request_query_parsing_and_overlap_boundaries() {
    let params = parse_create_delete_request_params(Some(
        "query=%7Bapp%3D%22api%22%7D&start=10&end=20&max_interval=1h",
    ))
    .unwrap();
    assert_eq!(params.query, r#"{app="api"}"#);
    assert_eq!(params.start_time, 10);
    assert_eq!(params.end_time, 20);
    assert!(parse_create_delete_request_params(Some("query=x&start=20&end=10")).is_err());
    // A window of zero width is allowed: "end before start" is the error,
    // not "end not after start".
    check!(
        parse_create_delete_request_params(Some("query=x&start=10&end=10")).is_ok(),
        "a start and end at the same instant"
    );
    // `max_interval` is parsed for its own sake -- the value is discarded,
    // so only an invalid one shows the parse happening at all. The case
    // above passes `1h`, which is accepted whether or not it is read.
    check!(
        parse_create_delete_request_params(Some(
            "query=x&start=10&end=20&max_interval=notaduration"
        ))
        .is_err(),
        "an unparseable max_interval is refused"
    );

    let list = parse_list_delete_requests_params(Some("start=10&end=20")).unwrap();
    assert_eq!(list.start_time, Some(10));
    assert_eq!(list.end_time, Some(20));
    assert!(parse_list_delete_requests_params(Some("start=10")).is_err());
    assert_eq!(
        parse_cancel_delete_request_params(Some("request_id=delete-1&force=true")).unwrap(),
        "delete-1"
    );
    assert!(parse_cancel_delete_request_params(Some("request_id=delete-1&force=maybe")).is_err());
    assert_eq!(
        parse_loki_delete_timestamp_query_param("start", "1.5").unwrap(),
        1
    );

    let request = CompactorDeleteRequest {
        tenant: "tenant-a".to_string(),
        request_id: "delete-1".to_string(),
        query: r#"{app="api"}"#.to_string(),
        start_time: 10,
        end_time: 20,
        status: "received".to_string(),
        created_at: 1,
    };
    for (filter, want) in [
        (list, true),
        (
            ListDeleteRequestsParams {
                start_time: Some(20),
                end_time: Some(30),
            },
            true,
        ),
        (
            ListDeleteRequestsParams {
                start_time: Some(21),
                end_time: Some(30),
            },
            false,
        ),
    ] {
        assert_eq!(delete_request_overlaps_filter(&request, &filter), want);
    }
    for (right, want) in [
        (TimeRange::new(20, 30).unwrap(), true),
        (TimeRange::new(21, 30).unwrap(), false),
    ] {
        assert_eq!(ranges_overlap(TimeRange::new(10, 20).unwrap(), right), want);
    }
}
