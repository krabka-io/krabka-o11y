use super::*;

struct CalendarQueryCase {
    query: &'static str,
    expected: f64,
}

#[tokio::test]
pub(crate) async fn instant_calendar_functions_extract_utc_fields_from_sample_values() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[
            crate::test_support::TestLabelPair::new("__name__", "event_timestamp_seconds"),
            crate::test_support::TestLabelPair::new("case", "leap"),
        ]),
        10_000,
        1_709_178_060.0,
    );

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    for case in [
        CalendarQueryCase {
            query: "year(event_timestamp_seconds)",
            expected: 2024.0,
        },
        CalendarQueryCase {
            query: "month(event_timestamp_seconds)",
            expected: 2.0,
        },
        CalendarQueryCase {
            query: "day_of_month(event_timestamp_seconds)",
            expected: 29.0,
        },
        CalendarQueryCase {
            query: "day_of_week(event_timestamp_seconds)",
            expected: 4.0,
        },
        CalendarQueryCase {
            query: "day_of_year(event_timestamp_seconds)",
            expected: 60.0,
        },
        CalendarQueryCase {
            query: "days_in_month(event_timestamp_seconds)",
            expected: 29.0,
        },
        CalendarQueryCase {
            query: "hour(event_timestamp_seconds)",
            expected: 3.0,
        },
        CalendarQueryCase {
            query: "minute(event_timestamp_seconds)",
            expected: 41.0,
        },
    ] {
        let samples = instant_vector(&engine, case.query, 10_000).await;
        assert_one_unnamed_float(
            &samples,
            ExpectedLabel {
                name: "case",
                label_value: "leap",
            },
            case.expected,
        );
    }
}
