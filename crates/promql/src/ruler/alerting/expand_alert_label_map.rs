use krabka_logql::{LineFormat, TemplateData, TemplateRenderError};

use super::BTreeMap;

/// Fixed variables shared by ruler notifications and HTTP alert presentation.
pub(crate) fn alert_template_variables(
    value: TemplateData,
    series_labels: &crate::PromqlLabels,
    external_labels: &krabka_blockstore::Labels,
    external_url: &str,
) -> BTreeMap<String, TemplateData> {
    let mut variables = BTreeMap::new();
    variables.insert("value".to_owned(), value);
    let mut series = BTreeMap::new();
    for pair in series_labels.iter() {
        series.insert(pair.name.clone(), pair.value.as_bytes().to_vec());
    }
    variables.insert("labels".to_owned(), TemplateData::ByteLabels(series));
    let mut external = BTreeMap::new();
    for pair in external_labels.iter() {
        external.insert(pair.name.to_owned(), pair.value.as_bytes().to_vec());
    }
    variables.insert(
        "externalLabels".to_owned(),
        TemplateData::ByteLabels(external),
    );
    variables.insert(
        "externalURL".to_owned(),
        TemplateData::String(external_url.to_owned()),
    );
    variables
}

/// Expands only executed template queries at the rule's evaluation timestamp.
/// A missing query suspends rendering before its consumers run. Replaying with
/// the same variables, timestamp and resolved call results also supports queries whose
/// expressions depend on earlier query results or a branch's current labels.
pub(crate) async fn expand_alert_label_map_async<S: crate::MetricStore>(
    engine: &crate::PromqlEngine<S>,
    tenant: &krabka_blockstore::TenantId,
    map: &BTreeMap<String, String>,
    variables: &BTreeMap<String, TemplateData>,
    eval_time_ms: i64,
) -> BTreeMap<String, crate::PromqlString> {
    let mut expanded = BTreeMap::new();
    for entry in map {
        let name = &entry.0;
        let text = entry.1;
        let result = match LineFormat::new_prometheus(text) {
            Err(error) => Err(error.to_string()),
            Ok(format) => {
                render_alert_template(&format, variables, eval_time_ms, |query| async move {
                    engine
                        .query_instant(tenant, &query, eval_time_ms)
                        .await
                        .map_or_else(
                            |error| TemplateData::QueryError(error.to_string()),
                            super::template_query_value,
                        )
                })
                .await
            }
        };
        expanded.insert(
            name.to_string(),
            result
                .unwrap_or_else(|error| format!("<error expanding template: {error}>").into_bytes())
                .into(),
        );
    }
    expanded
}

/// Query results belong to executed call ordinals within one template. A
/// repeated query invokes the engine again, preserving fresh sample pointers.
async fn render_alert_template<F, Fut>(
    format: &LineFormat,
    variables: &BTreeMap<String, TemplateData>,
    eval_time_ms: i64,
    mut resolve: F,
) -> Result<Vec<u8>, String>
where
    F: FnMut(String) -> Fut,
    Fut: std::future::Future<Output = TemplateData>,
{
    // ponytail: replay work grows quadratically with executed query calls; use
    // async VM continuations when real rules need many calls.
    let mut results = Vec::new();
    loop {
        match format.render_prometheus_bytes(variables, &results, eval_time_ms) {
            Ok(bytes) => return Ok(bytes),
            Err(TemplateRenderError::Execution(error)) => return Err(error),
            Err(TemplateRenderError::NeedsQuery(query)) => results.push(resolve(query).await),
        }
    }
}

/// Synchronous fixture adapter for tests with already supplied query results.
#[cfg(test)]
pub(crate) fn expand_alert_label_map(
    map: &BTreeMap<String, String>,
    value: f64,
    series_labels: &crate::PromqlLabels,
    external_labels: &krabka_blockstore::Labels,
    external_url: &str,
    queries: &BTreeMap<String, TemplateData>,
) -> BTreeMap<String, crate::PromqlString> {
    let variables = alert_template_variables(
        TemplateData::Float(value),
        series_labels,
        external_labels,
        external_url,
    );
    let mut expanded_map = BTreeMap::new();
    for entry in map {
        let name = entry.0;
        let text = entry.1;
        let expanded = LineFormat::new_prometheus(text)
            .map_err(|error| error.to_string())
            .and_then(|format| format.render_bytes_with_variables_and_queries(&variables, queries))
            .unwrap_or_else(|error| format!("<error expanding template: {error}>").into_bytes());
        expanded_map.insert(name.clone(), expanded.into());
    }
    expanded_map
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::{
        BTreeMap, LineFormat, alert_template_variables, expand_alert_label_map_async,
        render_alert_template,
    };
    use crate::{
        EngineOpts, InMemoryMetricStore, PromqlEngine, PromqlLabels, test_support::tenant_id,
    };

    struct TemplateEntry {
        name: &'static str,
        source: &'static str,
    }

    struct ExpansionCase {
        key: &'static str,
        expected: &'static str,
    }

    fn template_map(entries: impl IntoIterator<Item = TemplateEntry>) -> BTreeMap<String, String> {
        let mut map = BTreeMap::new();
        for entry in entries {
            map.insert(entry.name.into(), entry.source.into());
        }
        map
    }

    #[tokio::test]
    async fn dynamic_and_dependent_queries_resume_only_executed_branches_at_fixed_time() {
        let mut store = InMemoryMetricStore::new();
        let labels = PromqlLabels::from_pairs([
            krabka_metrics::MetricLabelPair::new("__name__", "input"),
            krabka_metrics::MetricLabelPair::new("job", "api"),
        ]);
        let mut lookup = PromqlLabels::from_pairs([
            krabka_metrics::MetricLabelPair::new("__name__", "lookup"),
            krabka_metrics::MetricLabelPair::new("job", "api"),
        ]);
        lookup.insert("next_query", "nested{job=\"api\"}");
        store.push_float("tenant-a", lookup, 60_000, 7.0);
        store.push_float(
            "tenant-a",
            PromqlLabels::from_pairs([
                krabka_metrics::MetricLabelPair::new("__name__", "nested"),
                krabka_metrics::MetricLabelPair::new("job", "api"),
            ]),
            60_000,
            9.0,
        );
        store.push_float(
            "tenant-b",
            PromqlLabels::from_pairs([
                krabka_metrics::MetricLabelPair::new("__name__", "nested"),
                krabka_metrics::MetricLabelPair::new("job", "api"),
            ]),
            60_000,
            999.0,
        );
        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let map = template_map([
            TemplateEntry {
                name: "dynamic",
                source: r#"{{ query (printf "lookup{job=%q}" $labels.job) | first | value }}"#,
            },
            TemplateEntry {
                name: "dependent",
                source: r#"{{ if gt (query (printf "lookup{job=%q}" $labels.job) | first | value) 0.0 }}{{ query (query (printf "lookup{job=%q}" $labels.job) | first | label "next_query") | first | value }}{{ else }}wrong{{ end }}"#,
            },
            TemplateEntry {
                name: "unexecuted",
                source: r#"{{ if false }}{{ query "absent(" }}{{ else }}skipped{{ end }}"#,
            },
            TemplateEntry {
                name: "error",
                source: r#"{{ query "bad(" | first }}"#,
            },
            TemplateEntry {
                name: "clock",
                source: "{{ now }}",
            },
        ]);
        let variables = alert_template_variables(
            krabka_logql::TemplateData::Float(2.0),
            &labels,
            &krabka_blockstore::Labels::new(),
            "",
        );
        let expanded =
            expand_alert_label_map_async(&engine, &tenant_id("tenant-a"), &map, &variables, 60_000)
                .await;
        for case in [
            ExpansionCase {
                key: "dynamic",
                expected: "7",
            },
            ExpansionCase {
                key: "dependent",
                expected: "9",
            },
            ExpansionCase {
                key: "unexecuted",
                expected: "skipped",
            },
            ExpansionCase {
                key: "clock",
                expected: "60",
            },
        ] {
            assert2::assert!(
                expanded[case.key].as_bytes() == case.expected.as_bytes(),
                "{}: {:?}",
                case.key,
                expanded[case.key]
            );
        }
        assert2::assert!(
            expanded["error"]
                .as_str()
                .starts_with("<error expanding template: parse error:")
        );
        let requests = Arc::new(Mutex::new(Vec::new()));
        for text in map.values() {
            let format = LineFormat::new_prometheus(text).unwrap();
            let engine = &engine;
            let requests = Arc::clone(&requests);
            let _ = render_alert_template(&format, &variables, 60_000, |query| {
                let requests = Arc::clone(&requests);
                async move {
                    requests.lock().unwrap().push(query.clone());
                    engine
                        .query_instant(&tenant_id("tenant-a"), &query, 60_000)
                        .await
                        .map_or_else(
                            |error| krabka_logql::TemplateData::QueryError(error.to_string()),
                            super::super::template_query_value,
                        )
                }
            })
            .await;
        }
        assert2::assert!(
            *requests.lock().unwrap()
                == [
                    "lookup{job=\"api\"}",
                    "lookup{job=\"api\"}",
                    "nested{job=\"api\"}",
                    "lookup{job=\"api\"}",
                    "bad("
                ]
        );
        let replay =
            expand_alert_label_map_async(&engine, &tenant_id("tenant-a"), &map, &variables, 60_000)
                .await;
        assert2::assert!(replay == expanded);
    }
    #[tokio::test]
    async fn replay_restores_query_order_before_sorting_shared_slice_aliases() {
        use krabka_logql::TemplateData;

        use crate::{InstantSample, PromqlLabels, QueryResult, SampleValue};
        let vector = super::super::template_query_value(QueryResult::InstantVector(
            ["b", "a"]
                .into_iter()
                .map(|job| InstantSample {
                    labels: PromqlLabels::from_pairs([krabka_metrics::MetricLabelPair::new(
                        "job", job,
                    )]),
                    ts_ms: 60_000,
                    value: SampleValue::Float(1.0),
                    drop_name: false,
                })
                .collect(),
        ));
        let format = LineFormat::new_prometheus(r#"{{ $q := query "vector" }}{{ $before := label "job" (first $q) }}{{ $sorted := sortByLabel "job" $q }}{{ $found := query (printf "lookup{job=%q}" $before) | first | value }}{{ $before }}/{{ label "job" (first $q) }}/{{ label "job" (first $sorted) }}/{{ $found }}"#).unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let actual = render_alert_template(&format, &BTreeMap::new(), 60_000, |query| {
            let requests = Arc::clone(&requests);
            let vector = vector.clone();
            async move {
                requests.lock().unwrap().push(query.clone());
                match query.as_str() {
                    "vector" => vector,
                    "lookup{job=\"b\"}" => {
                        super::super::template_query_value(QueryResult::Scalar {
                            ts_ms: 60_000,
                            value: 7.0,
                        })
                    }
                    _ => TemplateData::QueryError(format!("unexpected query: {query}")),
                }
            }
        })
        .await
        .unwrap();
        assert2::assert!(actual == b"b/a/a/7");
        assert2::assert!(*requests.lock().unwrap() == ["vector", "lookup{job=\"b\"}"]);
        let TemplateData::QueryResult(original) = vector else {
            panic!("expected named vector")
        };
        let original = original.snapshot();
        let TemplateData::Sample(first) = &original[0] else {
            panic!("expected sample pointer")
        };
        assert2::assert!(
            matches!(first.get("Labels"),Some(TemplateData::ByteLabels(labels)) if labels["job"] == b"b")
        );
    }
}
