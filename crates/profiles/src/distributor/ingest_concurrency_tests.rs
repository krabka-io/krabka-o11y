use std::{
    sync::atomic::{AtomicBool, Ordering},
    time::{Duration, Instant},
};

use assert2::assert;
use tokio::sync::{Notify, Semaphore};

use super::{tests::test_state, *};

struct ControlledSink {
    block_once: AtomicBool,
    entered: Notify,
    release: Semaphore,
    fail: bool,
    records: Mutex<Vec<ProfileRecord>>,
}

impl ControlledSink {
    fn new(fail: bool) -> Self {
        Self {
            block_once: AtomicBool::new(true),
            entered: Notify::new(),
            release: Semaphore::new(0),
            fail,
            records: Mutex::default(),
        }
    }
}

#[async_trait::async_trait]
impl WalSink for ControlledSink {
    async fn append(&self, record: ProfileRecord) -> Result<(), ProfilesError> {
        if record.tenant == "tenant-a" && self.block_once.swap(false, Ordering::SeqCst) {
            self.entered.notify_one();
            self.release.acquire().await.unwrap().forget();
            if self.fail {
                return Err(ProfilesError::Produce("controlled failure".into()));
            }
        }
        self.records.lock().unwrap().push(record);
        Ok(())
    }
}

fn raw(value: i64, cumulative: bool) -> crate::ingest::RawProfile {
    let mut raw = crate::wire::test_fixtures::raw_profile_cpu();
    let mut profile = raw.profile.inner().clone();
    profile.sample[0].value[0] = value;
    raw.profile = PprofProfile::from(profile);
    raw.delta = cumulative;
    raw
}

async fn push(
    state: &DistributorState,
    tenant: &str,
    value: i64,
    cumulative: bool,
) -> Result<(), ProfilesError> {
    process_raw(
        state,
        &TenantId::new(tenant).unwrap(),
        vec![raw(value, cumulative)],
    )
    .await
}

fn spawn_push(
    state: &Arc<DistributorState>,
    tenant: &'static str,
    value: i64,
    cumulative: bool,
) -> tokio::task::JoinHandle<Result<(), ProfilesError>> {
    let state = state.clone();
    tokio::spawn(async move { push(&state, tenant, value, cumulative).await })
}

async fn entered(sink: &ControlledSink) {
    tokio::time::timeout(Duration::from_secs(2), sink.entered.notified())
        .await
        .unwrap();
}

#[tokio::test]
async fn independent_tenants_overlap_acknowledgements() {
    for (cumulative, max_series) in [(false, 0), (false, 10), (true, 0), (true, 10)] {
        let sink = Arc::new(ControlledSink::new(false));
        let state = Arc::new(test_state(
            sink.clone(),
            OverridesProvider::new(Limits {
                max_series,
                ..Limits::default()
            }),
        ));
        if cumulative {
            push(&state, "tenant-a", 10, true).await.unwrap();
            push(&state, "tenant-b", 10, true).await.unwrap();
        }
        let a = spawn_push(&state, "tenant-a", 20, cumulative);
        entered(&sink).await;
        tokio::time::timeout(
            Duration::from_secs(2),
            push(&state, "tenant-b", 20, cumulative),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(!a.is_finished());
        sink.release.add_permits(1);
        a.await.unwrap().unwrap();
        assert_eq!(sink.records.lock().unwrap().len(), 2);
        if cumulative {
            // Both commits survive even though each staged before the other committed.
            let cache = state.cumulative_profiles.lock().await;
            assert_eq!(cache.len(), 2);
            assert!(
                cache
                    .values()
                    .all(|samples| samples.values().all(|value| *value == 20))
            );
        }
    }
}

#[tokio::test]
async fn ordinary_ingestion_does_not_access_cumulative_state_or_coordination() {
    let sink = Arc::new(ControlledSink::new(false));
    sink.block_once.store(false, Ordering::SeqCst);
    let state = test_state(sink.clone(), OverridesProvider::new(Limits::default()));
    let _cache = state.cumulative_profiles.lock().await;
    let _tenant = state
        .tenant_ingest_lock("tenant-a")
        .unwrap()
        .lock_owned()
        .await;
    tokio::time::timeout(Duration::from_secs(2), push(&state, "tenant-a", 10, false))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(sink.records.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn same_series_waits_for_commit_and_failed_append_keeps_baseline() {
    for fail in [false, true] {
        let sink = Arc::new(ControlledSink::new(fail));
        let state = Arc::new(test_state(
            sink.clone(),
            OverridesProvider::new(Limits::default()),
        ));
        push(&state, "tenant-a", 10, true).await.unwrap();
        let first = spawn_push(&state, "tenant-a", 20, true);
        entered(&sink).await;
        let mut second = Box::pin(push(&state, "tenant-a", 30, true));
        assert!(futures::poll!(&mut second).is_pending());
        sink.release.add_permits(1);
        assert_eq!(first.await.unwrap().is_err(), fail);
        second.await.unwrap();
        let records = sink.records.lock().unwrap();
        let values: Vec<_> = records
            .iter()
            .map(|record| record.samples[0].value)
            .collect();
        assert_eq!(values, if fail { vec![20] } else { vec![10, 10] });
    }
}

#[tokio::test]
async fn failed_reservation_cannot_remove_a_successful_same_series_write() {
    let sink = Arc::new(ControlledSink::new(true));
    let state = Arc::new(test_state(
        sink.clone(),
        OverridesProvider::new(Limits {
            max_series: 1,
            ..Limits::default()
        }),
    ));
    let first = spawn_push(&state, "tenant-a", 10, false);
    entered(&sink).await;
    let mut second = Box::pin(push(&state, "tenant-a", 20, false));
    assert!(futures::poll!(&mut second).is_pending());
    sink.release.add_permits(1);
    assert!(first.await.unwrap().is_err());
    second.await.unwrap();
    assert_eq!(state.active_series.lock().unwrap()["tenant-a"].len(), 1);
    let mut other = raw(30, false);
    other.labels.insert("service_name", "other");
    assert!(
        process_raw(&state, &TenantId::new("tenant-a").unwrap(), vec![other])
            .await
            .is_err()
    );
}

struct PartialSink {
    remaining: std::sync::atomic::AtomicUsize,
}

#[async_trait::async_trait]
impl WalSink for PartialSink {
    async fn append(&self, _record: ProfileRecord) -> Result<(), ProfilesError> {
        self.remaining
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
            .map(|_| ())
            .map_err(|_| ProfilesError::Produce("partial failure".into()))
    }
}

#[tokio::test]
async fn partial_mixed_batch_rolls_back_staged_baselines_and_reservations() {
    let sink = Arc::new(PartialSink {
        remaining: std::sync::atomic::AtomicUsize::new(1),
    });
    let state = test_state(
        sink,
        OverridesProvider::new(Limits {
            max_series: 10,
            ..Limits::default()
        }),
    );
    // Seed one baseline. A second, new cumulative series is staged alongside
    // its delta and two ordinary records; the sink accepts only the delta.
    push(&state, "tenant-a", 10, true).await.unwrap();
    let mut new_series = raw(100, true);
    new_series.labels.insert("service_name", "new");
    let error = process_raw(
        &state,
        &TenantId::new("tenant-a").unwrap(),
        vec![raw(20, true), new_series, raw(5, false), raw(6, false)],
    )
    .await
    .unwrap_err();
    assert!(matches!(
        error,
        ProfilesError::ProduceBatch {
            appended: 1,
            total: 3,
            ..
        }
    ));
    let cache = state.cumulative_profiles.lock().await;
    assert_eq!(cache.len(), 1);
    assert!(
        cache
            .values()
            .all(|samples| samples.values().all(|value| *value == 10))
    );
    assert_eq!(state.active_series.lock().unwrap()["tenant-a"].len(), 1);
    assert_eq!(state.metrics.wal_produce.partial_batch_appends(), 1);
}

struct DelayedSink(Duration);

#[async_trait::async_trait]
impl WalSink for DelayedSink {
    async fn append(&self, _record: ProfileRecord) -> Result<(), ProfilesError> {
        if !self.0.is_zero() {
            tokio::time::sleep(self.0).await;
        }
        Ok(())
    }
}

/// Run with `cargo test -p krabka-profiles --lib ingest_throughput -- --ignored --nocapture`.
/// Measures the full decode/stage/append path, varying independent tenants,
/// simulated acknowledgement latency, and unrelated cached cumulative series.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "throughput benchmark"]
async fn ingest_throughput() {
    for cumulative in [false, true] {
        for tenants in [1, 8, 32] {
            for delay_ms in [0, 5, 20] {
                for cache_size in [0, 1_000, 10_000] {
                    let state = Arc::new(test_state(
                        Arc::new(DelayedSink(Duration::from_millis(delay_ms))),
                        OverridesProvider::new(Limits::default()),
                    ));
                    {
                        let mut cache = state.cumulative_profiles.lock().await;
                        for id in 0..cache_size {
                            cache.insert(
                                CumulativeProfileKey {
                                    tenant: format!("cached-{id}"),
                                    labels: vec![],
                                },
                                (0..100).map(|frame| (vec![frame], 10)).collect(),
                            );
                        }
                    }
                    if cumulative {
                        for id in 0..tenants {
                            push(&state, &format!("tenant-{id}"), 1, true)
                                .await
                                .unwrap();
                        }
                    }
                    let start = Instant::now();
                    let mut tasks = Vec::new();
                    for id in 0..tenants {
                        let state = state.clone();
                        tasks.push(tokio::spawn(async move {
                            for value in 2..22 {
                                push(&state, &format!("tenant-{id}"), value, cumulative)
                                    .await
                                    .unwrap();
                            }
                        }));
                    }
                    for task in tasks {
                        task.await.unwrap();
                    }
                    println!(
                        "cumulative={cumulative} tenants={tenants} ack_ms={delay_ms} cache={cache_size} requests_per_sec={:.1}",
                        f64::from(tenants * 20) / start.elapsed().as_secs_f64()
                    );
                }
            }
        }
    }
}

#[tokio::test]
async fn repeated_series_in_one_request_uses_the_staged_baseline() {
    let sink = Arc::new(ControlledSink::new(false));
    sink.block_once.store(false, Ordering::SeqCst);
    let state = test_state(sink.clone(), OverridesProvider::new(Limits::default()));
    process_raw(
        &state,
        &TenantId::new("tenant-a").unwrap(),
        vec![raw(10, true), raw(20, true), raw(35, true)],
    )
    .await
    .unwrap();
    let values: Vec<_> = sink
        .records
        .lock()
        .unwrap()
        .iter()
        .map(|record| record.samples[0].value)
        .collect();
    assert_eq!(values, vec![10, 15]);
    push(&state, "tenant-a", 40, true).await.unwrap();
    assert_eq!(
        sink.records.lock().unwrap().last().unwrap().samples[0].value,
        5
    );
}

#[test]
fn tenant_lock_cleanup_preserves_live_locks_and_reclaims_idle_entries() {
    let mut state = test_state(
        Arc::new(ControlledSink::new(false)),
        OverridesProvider::new(Limits::default()),
    );
    state.max_tracked_tenants = 1;
    let a = state.tenant_ingest_lock("tenant-a").unwrap();
    let b = state.tenant_ingest_lock("tenant-b").unwrap();
    assert!(Arc::ptr_eq(
        &a,
        &state.tenant_ingest_lock("tenant-a").unwrap()
    ));
    drop(b);
    let _c = state.tenant_ingest_lock("tenant-c").unwrap();
    assert!(
        !state
            .tenant_ingest_locks
            .lock()
            .unwrap()
            .contains_key("tenant-b")
    );
    assert!(Arc::ptr_eq(
        &a,
        &state.tenant_ingest_lock("tenant-a").unwrap()
    ));
}

#[tokio::test]
async fn rate_rejection_keeps_baselines_and_releases_new_reservations() {
    let sink = Arc::new(ControlledSink::new(false));
    let state = test_state(
        sink.clone(),
        OverridesProvider::new(Limits {
            max_series: 10,
            ingestion_rate: Frequency::from_per_sec(1.0),
            ingestion_burst_profiles: 1,
            ..Limits::default()
        }),
    );
    push(&state, "tenant-a", 10, true).await.unwrap();
    let mut new_series = raw(100, true);
    new_series.labels.insert("service_name", "new");
    let error = process_raw(
        &state,
        &TenantId::new("tenant-a").unwrap(),
        vec![raw(20, true), new_series, raw(5, false)],
    )
    .await
    .unwrap_err();
    assert!(
        error.to_string().contains("ingestion rate exceeded"),
        "{error}"
    );
    assert!(sink.records.lock().unwrap().is_empty());
    let cache = state.cumulative_profiles.lock().await;
    assert_eq!(cache.len(), 1);
    assert!(
        cache
            .values()
            .all(|samples| samples.values().all(|value| *value == 10))
    );
    assert_eq!(state.active_series.lock().unwrap()["tenant-a"].len(), 1);
}
