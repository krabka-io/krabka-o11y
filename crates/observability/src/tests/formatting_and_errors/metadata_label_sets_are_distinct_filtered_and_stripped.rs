use super::*;

/// `metadata_label_sets` lists the distinct label sets a tenant has,
/// filtered by the request's matchers and hiding the labels that are
/// internal. Replacing its body with an empty list passed the whole suite
/// before this test, so every part of it is pinned here.
#[tokio::test]
pub(crate) async fn metadata_label_sets_are_distinct_filtered_and_stripped() {
    async fn sets(state: &QuerierState, matchers: Vec<String>) -> Vec<Labels> {
        let params = SeriesParams {
            matchers,
            start: None,
            end: None,
            since: None,
        };
        super::super::prelude::metadata_label_sets(state, "t", &params)
            .await
            .expect("readable")
    }
    let mut label_index = LabelIndex::default();
    let labels = |pairs: &[(&str, &str)]| {
        pairs
            .iter()
            .map(|(name, value)| ((*name).to_string(), (*value).to_string()))
            .collect::<Labels>()
    };
    // Two series for one tenant plus one for another, and a fourth that
    // differs from the first only by an internal label -- so it collapses
    // onto it once that label is hidden.
    label_index.insert_series("t", labels(&[("app", "web"), ("env", "prod")]));
    label_index.insert_series("t", labels(&[("app", "api"), ("env", "prod")]));
    label_index.insert_series(
        "t",
        labels(&[("app", "web"), ("env", "prod"), ("detected_level", "warn")]),
    );
    label_index.insert_series("other", labels(&[("app", "elsewhere")]));

    let dir = tempfile::TempDir::new().expect("temp dir");
    let state = QuerierState::new(dir.path(), label_index, BlockIndex::default());

    // Unfiltered: the two distinct visible sets, with the third collapsed
    // onto the first because its only difference is hidden.
    let all = sets(&state, Vec::new()).await;
    check!(all.len() == 2, "got {all:?}");
    check!(
        all.iter().all(|set| set.get("detected_level").is_none()),
        "the internal label is stripped, not reported"
    );

    // Another tenant's series are not this tenant's.
    check!(
        all.iter()
            .all(|set| set.get("app").map(String::as_str) != Some("elsewhere")),
        "tenant isolation"
    );

    // A matcher narrows the result rather than being ignored.
    let web = sets(&state, vec![r#"{app="web"}"#.to_string()]).await;
    check!(web.len() == 1, "got {web:?}");
    check!(web[0].get("app").map(String::as_str) == Some("web"));

    let none = sets(&state, vec![r#"{app="absent"}"#.to_string()]).await;
    check!(
        none.is_empty(),
        "a matcher that matches nothing returns nothing"
    );
}
