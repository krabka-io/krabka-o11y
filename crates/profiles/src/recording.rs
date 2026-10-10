//! Pyroscope recording-rule evaluation and Prometheus remote-write export.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::Arc,
    time::Duration,
};

use arrow::array::{Int64Array, UInt64Array};
use futures::{StreamExt as _, TryStreamExt as _};
use krabka_blockstore::{
    BlockLevel, BlockMeta, COL_FINGERPRINT, COL_TIMESTAMP, DEFAULT_BLOCK_READ_MAX,
    MERGE_READ_BATCH_ROWS, PCOL_STACKTRACE_ID, PCOL_STACKTRACE_PARTITION, PCOL_VALUE, ProfileIndex,
    SeriesFingerprint, open_block_stream,
};
use krabka_metrics::wire::pb::v1::{Label, Sample, TimeSeries, WriteRequest};
use krabka_pprof::{SymbolDb, parse_label_selector};
use object_store::{ObjectStore, ObjectStoreExt as _, PutMode, PutOptions, path::Path};
use prost::Message as _;

use crate::{ProfilesError, wire::pb};

const RULES_PREFIX: &str = "profiles-admin";
const EXPORT_PREFIX: &str = "profiles-admin/recording-exports";

/// Shared object-store marker advertised by query roles when recording export is configured.
pub const RECORDING_RULES_ENABLED_KEY: &str = "profiles-admin/recording-rules-enabled";

#[derive(Clone, PartialEq, prost::Message)]
struct PendingExport {
    #[prost(string, tag = "1")]
    tenant: String,
    #[prost(bytes, tag = "2")]
    body: Vec<u8>,
}

/// Evaluate newly-ingested blocks and export one sample per rule, group and
/// profile timestamp through Prometheus remote write.
///
/// Higher levels are rewrites of data already evaluated at ingestion. Skipping
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
    let mut first_error = None;

    let pending = store
        .list(Some(&Path::from(format!("{EXPORT_PREFIX}/"))))
        .try_collect::<Vec<_>>()
        .await
        .map_err(recording_error)?;
    for object in pending
        .into_iter()
        .filter(|object| object.location.as_ref().ends_with(".pending"))
    {
        let result = retry_pending(store, &client, remote_write_url, &object.location).await;
        match result {
            Ok(()) => exported += 1,
            Err(error) => {
                first_error.get_or_insert(error);
            }
        }
    }

    for block in blocks
        .iter()
        .filter(|block| block.level == BlockLevel::INGESTED)
    {
        let pending_key = export_key(block, "pending");
        let done_key = export_key(block, "done");
        if object_exists(store.as_ref(), &done_key).await?
            || object_exists(store.as_ref(), &pending_key).await?
        {
            continue;
        }
        let rules = load_rules(store.as_ref(), &block.tenant).await?;
        if rules.rules.is_empty() {
            store
                .put(&done_key, Vec::new().into())
                .await
                .map_err(recording_error)?;
            continue;
        }
        let timeseries = evaluate_block(store, index, block, &rules.rules).await?;
        if timeseries.is_empty() {
            store
                .put(&done_key, Vec::new().into())
                .await
                .map_err(recording_error)?;
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
        match store
            .put_opts(
                &pending_key,
                PendingExport {
                    tenant: block.tenant.clone(),
                    body,
                }
                .encode_to_vec()
                .into(),
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await
        {
            Ok(_) => {}
            Err(object_store::Error::AlreadyExists { .. }) => continue,
            Err(error) => return Err(recording_error(error)),
        }
        match retry_pending(store, &client, remote_write_url, &pending_key).await {
            Ok(()) => exported += 1,
            Err(error) => {
                first_error.get_or_insert(error);
            }
        }
    }
    first_error.map_or(Ok(exported), Err)
}

async fn object_exists(store: &dyn ObjectStore, key: &Path) -> Result<bool, ProfilesError> {
    match store.head(key).await {
        Ok(_) => Ok(true),
        Err(object_store::Error::NotFound { .. }) => Ok(false),
        Err(error) => Err(recording_error(error)),
    }
}

fn export_key(block: &BlockMeta, suffix: &str) -> Path {
    Path::from(format!(
        "{EXPORT_PREFIX}/{}.{}",
        hex::encode(block.object_key.as_bytes()),
        suffix
    ))
}

async fn retry_pending(
    store: &Arc<dyn ObjectStore>,
    client: &reqwest::Client,
    remote_write_url: &url::Url,
    pending_key: &Path,
) -> Result<(), ProfilesError> {
    let bytes = store
        .get(pending_key)
        .await
        .map_err(recording_error)?
        .bytes()
        .await
        .map_err(recording_error)?;
    let pending = PendingExport::decode(bytes).map_err(recording_error)?;
    let response = client
        .post(remote_write_url.clone())
        .header("content-encoding", "snappy")
        .header("content-type", "application/x-protobuf")
        .header("user-agent", "krabka-profiles-recording-rules")
        .header("x-prometheus-remote-write-version", "0.1.0")
        .header(krabka_blockstore::TENANT_HEADER, &pending.tenant)
        .body(pending.body)
        .send()
        .await
        .map_err(recording_error)?;
    if !response.status().is_success() {
        return Err(recording_error(format!(
            "remote write returned {}",
            response.status()
        )));
    }
    let done_key = Path::from(pending_key.as_ref().replace(".pending", ".done"));
    store
        .put(&done_key, Vec::new().into())
        .await
        .map_err(recording_error)?;
    store.delete(pending_key).await.map_err(recording_error)
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
    let function_names: BTreeSet<_> = rules.iter().filter_map(recording_function_name).collect();
    let totals = block_totals(store, &block.object_key, &function_names).await?;
    let available: BTreeSet<_> = block.fingerprints.iter().copied().collect();
    let mut output = Vec::new();
    for rule in rules {
        let rule_totals = recording_function_name(rule)
            .map_or(&totals.unfiltered, |name| &totals.functions[name]);
        let mut matchers = Vec::new();
        for selector in &rule.matchers {
            matchers.extend(parse_label_selector(selector).map_err(recording_error)?);
        }
        let matching = index
            .matching_fingerprints(&block.tenant, &matchers)
            .map_err(recording_error)?;
        let mut groups = BTreeMap::<Vec<(String, String)>, BTreeMap<i64, i64>>::new();
        for fingerprint in matching.intersection(&available) {
            let Some(total) = rule_totals.get(fingerprint) else {
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
            let group = groups.entry(exported.into_iter().collect()).or_default();
            for (timestamp, value) in total {
                *group.entry(*timestamp).or_default() += value;
            }
        }
        output.extend(groups.into_iter().map(|(labels, values)| {
            TimeSeries {
                labels: labels
                    .into_iter()
                    .map(|(name, value)| Label { name, value })
                    .collect(),
                samples: values
                    .into_iter()
                    .map(|(timestamp, value)| Sample {
                        #[allow(clippy::cast_precision_loss)]
                        value: value as f64,
                        timestamp,
                    })
                    .collect(),
                ..Default::default()
            }
        }));
    }
    Ok(output)
}

type ProfileTotals = HashMap<SeriesFingerprint, BTreeMap<i64, i64>>;

struct BlockTotals {
    unfiltered: ProfileTotals,
    functions: HashMap<String, ProfileTotals>,
}

fn recording_function_name(rule: &pb::settings::v1::RecordingRule) -> Option<&str> {
    rule.stacktrace_filter
        .as_ref()?
        .function_name
        .as_ref()
        .map(|filter| filter.function_name.as_str())
        .filter(|name| !name.is_empty())
}

async fn block_totals(
    store: &Arc<dyn ObjectStore>,
    object_key: &str,
    function_names: &BTreeSet<&str>,
) -> Result<BlockTotals, ProfilesError> {
    let symbols = if function_names.is_empty() {
        None
    } else {
        let bytes = store
            .get(&Path::from(format!("{object_key}.symdb")))
            .await
            .map_err(recording_error)?
            .bytes()
            .await
            .map_err(recording_error)?;
        Some(SymbolDb::decode(&bytes).map_err(recording_error)?)
    };
    let mut matching_stacks = HashMap::<(u64, u32), BTreeSet<String>>::new();
    let (_, _, mut batches) = open_block_stream(
        Arc::clone(store),
        object_key,
        DEFAULT_BLOCK_READ_MAX,
        MERGE_READ_BATCH_ROWS,
    )
    .await
    .map_err(recording_error)?;
    let mut totals = BlockTotals {
        unfiltered: ProfileTotals::new(),
        functions: function_names
            .iter()
            .map(|name| ((*name).to_string(), ProfileTotals::new()))
            .collect(),
    };
    while let Some(batch) = batches.next().await {
        let batch = batch.map_err(recording_error)?;
        let fingerprints = batch
            .column_by_name(COL_FINGERPRINT)
            .and_then(|array| array.as_any().downcast_ref::<UInt64Array>())
            .ok_or_else(|| recording_error("profile block has no fingerprint column"))?;
        // Blocks contain one row per stack sample. Profile totals repeat on
        // every row, while sample values add across distinct same-time profiles.
        let values = batch
            .column_by_name(PCOL_VALUE)
            .and_then(|array| array.as_any().downcast_ref::<Int64Array>())
            .ok_or_else(|| recording_error("profile block has no value column"))?;
        let timestamps = batch
            .column_by_name(COL_TIMESTAMP)
            .and_then(|array| array.as_any().downcast_ref::<Int64Array>())
            .ok_or_else(|| recording_error("profile block has no timestamp column"))?;
        for row in 0..batch.num_rows() {
            totals
                .unfiltered
                .entry(fingerprints.value(row))
                .or_default()
                .entry(timestamps.value(row))
                .and_modify(|total| *total += values.value(row))
                .or_insert_with(|| values.value(row));
        }
        if let Some(symbols) = &symbols {
            add_function_totals(
                &batch,
                symbols,
                function_names,
                &mut matching_stacks,
                &mut totals.functions,
            )?;
        }
    }
    Ok(totals)
}

fn add_function_totals(
    batch: &arrow::record_batch::RecordBatch,
    symbols: &SymbolDb,
    function_names: &BTreeSet<&str>,
    matching_stacks: &mut HashMap<(u64, u32), BTreeSet<String>>,
    functions: &mut HashMap<String, ProfileTotals>,
) -> Result<(), ProfilesError> {
    let fingerprints = batch
        .column_by_name(COL_FINGERPRINT)
        .and_then(|array| array.as_any().downcast_ref::<UInt64Array>())
        .ok_or_else(|| recording_error("profile block has no fingerprint column"))?;
    let timestamps = batch
        .column_by_name(COL_TIMESTAMP)
        .and_then(|array| array.as_any().downcast_ref::<Int64Array>())
        .ok_or_else(|| recording_error("profile block has no timestamp column"))?;
    let column = |name| {
        batch
            .column_by_name(name)
            .and_then(|array| array.as_any().downcast_ref::<UInt64Array>())
            .ok_or_else(|| recording_error(format!("profile block has no {name} column")))
    };
    let partitions = column(PCOL_STACKTRACE_PARTITION)?;
    let stacks = column(PCOL_STACKTRACE_ID)?;
    let values = batch
        .column_by_name(PCOL_VALUE)
        .and_then(|array| array.as_any().downcast_ref::<Int64Array>())
        .ok_or_else(|| recording_error("profile block has no value column"))?;
    for row in 0..batch.num_rows() {
        let stack = u32::try_from(stacks.value(row)).map_err(recording_error)?;
        let matching = matching_stacks
            .entry((partitions.value(row), stack))
            .or_insert_with(|| {
                function_names
                    .iter()
                    .filter(|name| {
                        symbols.stacktrace_contains_function(partitions.value(row), stack, name)
                    })
                    .map(|name| (*name).to_string())
                    .collect()
            });
        // Pyroscope 2.3.1 metrics/observer.go matches any exact frame,
        // including inline frames. A set counts recursive occurrences
        // once per stack sample. Keep zeroes for matched series too.
        for (name, function_totals) in &mut *functions {
            let total = function_totals
                .entry(fingerprints.value(row))
                .or_default()
                .entry(timestamps.value(row))
                .or_default();
            if matching.contains(name) {
                *total += values.value(row);
            }
        }
    }
    Ok(())
}

fn recording_error(error: impl std::fmt::Display) -> ProfilesError {
    ProfilesError::Block(format!("recording-rule evaluation: {error}"))
}

#[cfg(test)]
mod tests {
    mod function_filters;

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
        test_support::{CpuRecord, cpu_record},
        wal::{WalFunction, WalLocation, WalSample},
    };

    const PROFILE_TYPE: &str = "process_cpu:cpu:nanoseconds:cpu:nanoseconds";
    type Capture = (HeaderMap, Bytes);

    #[tokio::test]
    async fn ingested_block_retries_and_preserves_profile_timestamps() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut first = profile_record(3);
        first.samples.push(WalSample {
            stacktrace_location_refs: vec![0],
            value: 4,
            timestamp_ns: 1_000_000,
            span_id: None,
            trace_id: None,
        });
        let mut second = profile_record(11);
        second.samples[0].timestamp_ns = 2_000_000;
        let block = build_block(
            &store,
            "tenant-a",
            0,
            &[first.clone(), second],
            (0, 0),
            &krabka_blockstore::ObjectStoreMetrics::unregistered(),
        )
        .await
        .unwrap()
        .remove(0);
        let labels = Labels::from_pairs(first.labels.iter().cloned());
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
                stacktrace_filter: Some(pb::settings::v1::StacktraceFilter {
                    function_name: Some(pb::settings::v1::StacktraceFilterFunctionName {
                        function_name: "main".into(),
                        metric_type: 0,
                    }),
                }),
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
        let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let app = Router::new()
            .route(
                "/write",
                post(
                    |Extension(tx): Extension<tokio::sync::mpsc::UnboundedSender<Capture>>,
                     Extension(attempts): Extension<Arc<std::sync::atomic::AtomicUsize>>,
                     headers: HeaderMap,
                     body: Bytes| async move {
                        tx.send((headers, body)).unwrap();
                        if attempts.fetch_add(1, std::sync::atomic::Ordering::Relaxed) == 0 {
                            StatusCode::INTERNAL_SERVER_ERROR
                        } else {
                            StatusCode::NO_CONTENT
                        }
                    },
                ),
            )
            .layer(Extension(tx))
            .layer(Extension(attempts));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url =
            url::Url::parse(&format!("http://{}/write", listener.local_addr().unwrap())).unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        assert!(
            evaluate_compacted_blocks(&store, &index, std::slice::from_ref(&block), &url)
                .await
                .is_err()
        );
        rx.recv().await.unwrap();
        assert!(
            evaluate_compacted_blocks(&store, &index, std::slice::from_ref(&block), &url)
                .await
                .unwrap()
                == 1
        );
        let (headers, body) = rx.recv().await.unwrap();
        assert!(headers[krabka_blockstore::TENANT_HEADER] == "tenant-a");
        let decoded = snap::raw::Decoder::new().decompress_vec(&body).unwrap();
        let request = WriteRequest::decode(decoded.as_slice()).unwrap();
        let series = &request.timeseries[0];
        assert!(series.samples.len() == 2);
        assert!(series.samples[0].timestamp == 1);
        assert!((series.samples[0].value - 7.0).abs() < f64::EPSILON);
        assert!(series.samples[1].timestamp == 2);
        assert!((series.samples[1].value - 11.0).abs() < f64::EPSILON);
        assert!(series.labels.iter().any(|label| {
            label.name == "__name__" && label.value == "profiles_recorded_cpu_total"
        }));
        assert!(
            series
                .labels
                .iter()
                .any(|label| { label.name == "service_name" && label.value == "api" })
        );
        assert!(
            evaluate_compacted_blocks(&store, &index, &[block], &url)
                .await
                .unwrap()
                == 0
        );
        server.abort();
    }

    fn profile_record(value: i64) -> ProfileRecord {
        cpu_record(CpuRecord {
            tenant: "tenant-a",
            service: "api",
            stack: vec![0],
            value,
            timestamp_ns: 1_000_000,
            function: "main",
        })
    }
}
