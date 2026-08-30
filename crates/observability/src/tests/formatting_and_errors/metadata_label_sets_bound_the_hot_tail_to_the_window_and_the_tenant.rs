use super::*;

/// The hot tail is bounded twice over: to the requesting tenant, and to
/// the requested window at both edges inclusively. Nothing had ever read
/// it through this endpoint, so a record from another tenant, one before
/// the window and one after it were all free to be reported, and the two
/// edges were free to exclude a record sitting exactly on them.
#[tokio::test]
pub(crate) async fn metadata_label_sets_bound_the_hot_tail_to_the_window_and_the_tenant() {
    // A hot tail is allowed to answer a range query with a *superset* --
    // the trait says so, because a coarse time index returns whole buckets
    // -- and the caller re-applies the exact bound. This one returns
    // everything, which is the widest superset there is.
    struct CoarseHotTail(Vec<super::super::prelude::WalLogRecord>);
    impl super::super::prelude::LogHotTail for CoarseHotTail {
        fn records(&self) -> Vec<super::super::prelude::WalLogRecord> {
            self.0.clone()
        }
        fn records_in_range(
            &self,
            _start_ns: i64,
            _end_ns: i64,
        ) -> Vec<super::super::prelude::WalLogRecord> {
            self.0.clone()
        }
    }

    let record = |tenant: &str, app: &str, timestamp_ns: i64| super::super::prelude::WalLogRecord {
        tenant: tenant.to_string(),
        labels: [("app".to_string(), app.to_string())]
            .into_iter()
            .collect::<Labels>(),
        timestamp_ns,
        line: "line".to_string(),
        structured_metadata: std::collections::BTreeMap::new(),
        position: None,
    };
    let sink = CoarseHotTail(vec![
        record("t", "on_the_start", 100),
        record("t", "inside", 150),
        record("t", "on_the_end", 200),
        record("t", "before", 99),
        record("t", "after", 201),
        record("other", "foreign", 150),
    ]);

    let dir = tempfile::TempDir::new().expect("temp dir");
    let state = QuerierState::new(dir.path(), LabelIndex::default(), BlockIndex::default())
        .with_hot_tail(sink, 0);

    let params = SeriesParams {
        matchers: Vec::new(),
        start: Some(100),
        end: Some(200),
        since: None,
    };
    let sets = super::super::prelude::metadata_label_sets(&state, "t", &params)
        .await
        .expect("readable");

    let mut apps = sets
        .iter()
        .filter_map(|set| set.get("app").map(String::as_str))
        .collect::<Vec<_>>();
    apps.sort_unstable();
    check!(
        apps == vec!["inside", "on_the_end", "on_the_start"],
        "got {apps:?}"
    );
}
