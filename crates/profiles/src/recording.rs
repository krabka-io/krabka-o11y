//! Pyroscope recording-rule evaluation and Prometheus remote-write export.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Int64Array, UInt64Array};
use futures::StreamExt as _;
use krabka_blockstore::{
    BlockLevel, BlockMeta, COL_FINGERPRINT, DEFAULT_BLOCK_READ_MAX, MERGE_READ_BATCH_ROWS,
    PCOL_TOTAL_VALUE, ProfileIndex, SeriesFingerprint, open_block_stream,
};
use krabka_metrics::wire::pb::v1::{Label, Sample, TimeSeries, WriteRequest};
use krabka_pprof::parse_label_selector;
use object_store::{ObjectStore, ObjectStoreExt as _, path::Path};
use prost::Message as _;

use crate::{ProfilesError, wire::pb};

const RULES_PREFIX: &str = "profiles-admin";

/// Evaluate newly-created level-one blocks and export one sample per rule and
/// group through Prometheus remote write.
///
/// Higher levels are rewrites of data already evaluated at level one. Skipping
/// them prevents duplicate samples when a block climbs the compaction ladder.
///
/// # Errors
/// Returns an error when stored rules or profile blocks are malformed, or the
/// remote-write endpoint rejects the request.
pub async fn evaluate_compacted_blocks(
    store: &Arc<dyn ObjectStore>,
    index: &ProfileIndex,
    blocks: &[BlockMeta],
    remote_write_url: &url::Url,
) -> Result<usize, ProfilesError> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .map_err(recording_error)?;
    let mut exported = 0;
    for block in blocks.iter().filter(|block| block.level == BlockLevel(1)) {
        let rules = load_rules(store.as_ref(), &block.tenant).await?;
        if rules.rules.is_empty() {
            continue;
        }
        let timeseries = evaluate_block(store, index, block, &rules.rules).await?;
        if timeseries.is_empty() {
            continue;
        }
        let body = snap::raw::Encoder::new()
            .compress_vec(
                &WriteRequest {
                    timeseries,
                    ..Default::default()
                }
                .encode_to_vec(),
            )
            .map_err(recording_error)?;
        let response = client
            .post(remote_write_url.clone())
            .header("content-encoding", "snappy")
            .header("content-type", "application/x-protobuf")
            .header("user-agent", "krabka-profiles-recording-rules")
            .header("x-prometheus-remote-write-version", "0.1.0")
            .header(krabka_blockstore::TENANT_HEADER, &block.tenant)
            .body(body)
            .send()
            .await
            .map_err(recording_error)?;
        if !response.status().is_success() {
            return Err(recording_error(format!(
                "remote write returned {}",
                response.status()
            )));
        }
        exported += 1;
    }
    Ok(exported)
}

async fn load_rules(
    store: &dyn ObjectStore,
    tenant: &str,
) -> Result<pb::settings::v1::ListRecordingRulesResponse, ProfilesError> {
    let key = Path::from(format!("{RULES_PREFIX}/{tenant}/recording-rules.pb"));
    let object = match store.get(&key).await {
        Ok(object) => object,
        Err(object_store::Error::NotFound { .. }) => {
            return Ok(pb::settings::v1::ListRecordingRulesResponse::default());
        }
        Err(error) => return Err(recording_error(error)),
    };
    let bytes = object.bytes().await.map_err(recording_error)?;
    pb::settings::v1::ListRecordingRulesResponse::decode(bytes).map_err(recording_error)
}

async fn evaluate_block(
    store: &Arc<dyn ObjectStore>,
    index: &ProfileIndex,
    block: &BlockMeta,
    rules: &[pb::settings::v1::RecordingRule],
) -> Result<Vec<TimeSeries>, ProfilesError> {
    let totals = block_totals(store, &block.object_key).await?;
    let available: BTreeSet<_> = block.fingerprints.iter().copied().collect();
    let mut output = Vec::new();
    for rule in rules.iter().filter(|rule| rule.stacktrace_filter.is_none()) {
        let mut matchers = Vec::new();
        for selector in &rule.matchers {
            matchers.extend(parse_label_selector(selector).map_err(recording_error)?);
        }
        let matching = index
            .matching_fingerprints(&block.tenant, &matchers)
            .map_err(recording_error)?;
        let mut groups = BTreeMap::<Vec<(String, String)>, i64>::new();
        for fingerprint in matching.intersection(&available) {
            let Some(total) = totals.get(fingerprint) else {
                continue;
            };
            let labels =
                index.series_for_fingerprints(&block.tenant, &BTreeSet::from([*fingerprint]), &[]);
            let Some(labels) = labels.first() else {
                continue;
            };
            let labels: HashMap<_, _> = labels.iter().cloned().collect();
            let mut exported = BTreeMap::new();
            for label in &rule.external_labels {
                if !matches!(label.name.as_str(), "__name__" | "profiles_rule_id") {
                    exported.insert(label.name.clone(), label.value.clone());
                }
            }
            for name in &rule.group_by {
                if let Some(value) = labels.get(name)
                    && !value.is_empty()
                {
                    exported.insert(name.clone(), value.clone());
                }
            }
            exported.insert("__name__".to_string(), rule.metric_name.clone());
            exported.insert("profiles_rule_id".to_string(), rule.id.clone());
            *groups.entry(exported.into_iter().collect()).or_default() += total;
        }
        output.extend(groups.into_iter().map(|(labels, value)| {
            TimeSeries {
                labels: labels
                    .into_iter()
                    .map(|(name, value)| Label { name, value })
                    .collect(),
                samples: vec![Sample {
                    #[allow(clippy::cast_precision_loss)]
                    value: value as f64,
                    timestamp: block.max_ts,
                }],
                ..Default::default()
            }
        }));
    }
    Ok(output)
}

async fn block_totals(
    store: &Arc<dyn ObjectStore>,
    object_key: &str,
) -> Result<HashMap<SeriesFingerprint, i64>, ProfilesError> {
    let (_, _, mut batches) = open_block_stream(
        Arc::clone(store),
        object_key,
        DEFAULT_BLOCK_READ_MAX,
        MERGE_READ_BATCH_ROWS,
    )
    .await
    .map_err(recording_error)?;
    let mut totals = HashMap::<SeriesFingerprint, i64>::new();
    while let Some(batch) = batches.next().await {
        let batch = batch.map_err(recording_error)?;
        let fingerprints = batch
            .column_by_name(COL_FINGERPRINT)
            .and_then(|array| array.as_any().downcast_ref::<UInt64Array>())
            .ok_or_else(|| recording_error("profile block has no fingerprint column"))?;
        let values = batch
            .column_by_name(PCOL_TOTAL_VALUE)
            .and_then(|array| array.as_any().downcast_ref::<Int64Array>())
            .ok_or_else(|| recording_error("profile block has no total-value column"))?;
        for row in 0..batch.num_rows() {
            *totals.entry(fingerprints.value(row)).or_default() += values.value(row);
        }
    }
    Ok(totals)
}

fn recording_error(error: impl std::fmt::Display) -> ProfilesError {
    ProfilesError::Block(format!("recording-rule evaluation: {error}"))
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use axum::{
        Extension, Router,
        body::Bytes,
        http::{HeaderMap, StatusCode},
        routing::post,
    };
    use krabka_blockstore::{BlockIndex as _, Labels};
    use object_store::memory::InMemory;

    use super::*;
    use crate::{
        ProfileRecord,
        blockbuilder::build_block,
        wal::{WalFunction, WalLocation, WalSample, WalSymbolSet},
    };

    const PROFILE_TYPE: &str = "process_cpu:cpu:nanoseconds:cpu:nanoseconds";
    type Capture = (HeaderMap, Bytes);

    #[tokio::test]
    async fn level_one_block_evaluates_and_remote_writes_by_tenant() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let record = profile_record(7);
        let mut block = build_block(
            &store,
            "tenant-a",
            0,
            std::slice::from_ref(&record),
            (0, 0),
            &krabka_blockstore::ObjectStoreMetrics::unregistered(),
        )
        .await
        .unwrap()
        .remove(0);
        block.level = BlockLevel(1);
        let labels = Labels::from_pairs(record.labels.iter().cloned());
        let mut index = ProfileIndex::new();
        index
            .add_series("tenant-a", labels.fingerprint(), &labels)
            .unwrap();
        index.add_block(&block);
        index.add_profile_block("tenant-a", &block.object_key, vec![0]);

        let rules = pb::settings::v1::ListRecordingRulesResponse {
            rules: vec![pb::settings::v1::RecordingRule {
                id: "abcdefghij".to_string(),
                metric_name: "profiles_recorded_cpu_total".to_string(),
                profile_type: PROFILE_TYPE.to_string(),
                matchers: vec![format!(r#"{{__profile_type__="{PROFILE_TYPE}"}}"#)],
                group_by: vec!["service_name".to_string()],
                generation: 1,
                ..Default::default()
            }],
        };
        store
            .put(
                &Path::from("profiles-admin/tenant-a/recording-rules.pb"),
                rules.encode_to_vec().into(),
            )
            .await
            .unwrap();

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Capture>();
        let app = Router::new()
            .route(
                "/write",
                post(
                    |Extension(tx): Extension<tokio::sync::mpsc::UnboundedSender<Capture>>,
                     headers: HeaderMap,
                     body: Bytes| async move {
                        tx.send((headers, body)).unwrap();
                        StatusCode::NO_CONTENT
                    },
                ),
            )
            .layer(Extension(tx));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url =
            url::Url::parse(&format!("http://{}/write", listener.local_addr().unwrap())).unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        assert!(
            evaluate_compacted_blocks(&store, &index, &[block], &url)
                .await
                .unwrap()
                == 1
        );
        let (headers, body) = rx.recv().await.unwrap();
        assert!(headers[krabka_blockstore::TENANT_HEADER] == "tenant-a");
        let decoded = snap::raw::Decoder::new().decompress_vec(&body).unwrap();
        let request = WriteRequest::decode(decoded.as_slice()).unwrap();
        let series = &request.timeseries[0];
        assert!((series.samples[0].value - 7.0).abs() < f64::EPSILON);
        assert!(series.labels.iter().any(|label| {
            label.name == "__name__" && label.value == "profiles_recorded_cpu_total"
        }));
        assert!(
            series
                .labels
                .iter()
                .any(|label| { label.name == "service_name" && label.value == "api" })
        );
        server.abort();
    }

    fn profile_record(value: i64) -> ProfileRecord {
        ProfileRecord {
            tenant: "tenant-a".to_string(),
            labels: vec![
                ("__name__".to_string(), "process_cpu".to_string()),
                ("__profile_type__".to_string(), PROFILE_TYPE.to_string()),
                ("service_name".to_string(), "api".to_string()),
            ],
            profile_type: PROFILE_TYPE.to_string(),
            samples: vec![WalSample {
                stacktrace_location_refs: vec![0],
                value,
                timestamp_ns: 1_000_000,
                span_id: None,
                trace_id: None,
            }],
            symbols: WalSymbolSet {
                strings: vec![String::new(), "main".to_string()],
                functions: vec![WalFunction {
                    name: 1,
                    system_name: 1,
                    filename: 0,
                    start_line: 0,
                }],
                locations: vec![WalLocation {
                    address: 0,
                    mapping_id: 0,
                    lines: vec![(0, 1)],
                }],
                mappings: Vec::new(),
            },
        }
    }
}
