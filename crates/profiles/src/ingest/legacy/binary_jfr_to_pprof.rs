use jfrs::reader::types::builtin::{StackTrace, ThreadState};

use super::{
    BTreeMap, Cursor, Deserialize, IngestQuery, JfrLabels, PprofProfile, ProfilesError,
    intern_string, jfr_method_name,
};

struct SampleTypeSpec {
    profile_type: &'static str,
    profile_unit: &'static str,
    metric: &'static str,
    period_type: &'static str,
    period_unit: &'static str,
}

macro_rules! sample_type {
    ($profile_type:expr, $profile_unit:expr, $metric:expr, $period_type:expr, $period_unit:expr) => {
        SampleTypeSpec {
            profile_type: $profile_type,
            profile_unit: $profile_unit,
            metric: $metric,
            period_type: $period_type,
            period_unit: $period_unit,
        }
    };
}

const TYPES: &[SampleTypeSpec] = &[
    sample_type!("cpu", "nanoseconds", "process_cpu", "cpu", "nanoseconds"),
    sample_type!("wall", "nanoseconds", "wall", "wall", "nanoseconds"),
    sample_type!(
        "alloc_in_new_tlab_objects",
        "count",
        "memory",
        "space",
        "bytes"
    ),
    sample_type!(
        "alloc_in_new_tlab_bytes",
        "bytes",
        "memory",
        "space",
        "bytes"
    ),
    sample_type!(
        "alloc_outside_tlab_objects",
        "count",
        "memory",
        "space",
        "bytes"
    ),
    sample_type!(
        "alloc_outside_tlab_bytes",
        "bytes",
        "memory",
        "space",
        "bytes"
    ),
    sample_type!("contentions", "count", "mutex", "mutex", "count"),
    sample_type!("delay", "nanoseconds", "mutex", "mutex", "count"),
    sample_type!("contentions", "count", "block", "block", "count"),
    sample_type!("delay", "nanoseconds", "block", "block", "count"),
    sample_type!("live", "count", "memory", "objects", "count"),
    sample_type!("alloc_sample_objects", "count", "memory", "space", "bytes"),
    sample_type!("alloc_sample_bytes", "bytes", "memory", "space", "bytes"),
    sample_type!("malloc_objects", "count", "memory", "", ""),
    sample_type!("malloc_bytes", "bytes", "memory", "", ""),
];

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct StackEvent<'a> {
    #[serde(borrow, default)]
    stack_trace: Option<StackTrace<'a>>,
    #[serde(borrow, default)]
    state: Option<ThreadState<'a>>,
    #[serde(default)]
    duration: i64,
    #[serde(default)]
    allocation_size: i64,
    #[serde(default)]
    tlab_size: i64,
    #[serde(default)]
    weight: i64,
    #[serde(default)]
    size: i64,
    #[serde(default = "one")]
    samples: i64,
    #[serde(default)]
    context_id: i64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ActiveSetting<'a> {
    #[serde(default)]
    name: Option<&'a str>,
    #[serde(default)]
    value: Option<&'a str>,
}

const fn one() -> i64 {
    1
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct StackFrame {
    name: String,
    line: i32,
}

type Stack = Vec<StackFrame>;

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct JfrSampleKey {
    type_index: usize,
    stack: Stack,
    labels: Vec<krabka_blockstore::LabelPair>,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct LocationKey {
    function_id: u64,
    line: i32,
}

pub(crate) fn binary_jfr_to_pprof(
    name: &str,
    raw: &[u8],
    query: &IngestQuery,
    labels: &JfrLabels,
) -> Result<PprofProfile, ProfilesError> {
    let period = (1_000_000_000_i64 / i64::from(query.sample_rate)).max(1);
    let mut event_mode = query.jfr_event.clone();
    let mut samples = BTreeMap::<JfrSampleKey, Vec<i64>>::new();
    let mut reader = jfrs::reader::JfrReader::new(Cursor::new(raw.to_vec()));

    for chunk in reader.chunks() {
        let chunk_parts =
            chunk.map_err(|err| ProfilesError::Decode(format!("jfr chunk decode: {err}")))?;
        let mut chunk_reader = chunk_parts.0;
        let chunk = chunk_parts.1;
        for event in chunk_reader.events(&chunk) {
            let event =
                event.map_err(|err| ProfilesError::Decode(format!("jfr event decode: {err}")))?;
            let class = event.class.name();
            if class == "jdk.ActiveSetting" {
                if let Ok(setting) = jfrs::reader::from_event::<ActiveSetting<'_>>(&event)
                    && setting.name == Some("event")
                    && let Some(value) = setting.value
                {
                    event_mode = value.to_string();
                }
                continue;
            }
            let Some(kind) = event_kind(class) else {
                continue;
            };
            if matches!(kind, EventKind::Execution) {
                let decoded: jfrs::reader::types::jdk::ExecutionSample<'_> =
                    jfrs::reader::from_event(&event).map_err(|err| {
                        ProfilesError::Decode(format!("jfr {class} decode: {err}"))
                    })?;
                let Some(stack) = decoded.stack_trace.as_ref().and_then(stack_frames) else {
                    continue;
                };
                if decoded.state.as_ref().and_then(|state| state.name) != Some("STATE_SLEEPING") {
                    add_sample(&mut samples, 0, &stack, &[], period);
                }
                if event_mode == "wall" {
                    add_sample(&mut samples, 1, &stack, &[], period);
                }
                continue;
            }
            let decoded: StackEvent<'_> = jfrs::reader::from_event(&event)
                .map_err(|err| ProfilesError::Decode(format!("jfr {class} decode: {err}")))?;
            let Some(stack) = decoded.stack_trace.as_ref().and_then(stack_frames) else {
                continue;
            };
            let mut context_labels = labels
                .contexts
                .get(&decoded.context_id)
                .cloned()
                .unwrap_or_default();
            context_labels.sort();

            match kind {
                EventKind::Execution => unreachable!("handled above"),
                EventKind::Wall => {
                    let value = decoded.samples.max(1).saturating_mul(period);
                    add_sample(&mut samples, 1, &stack, &context_labels, value);
                    if event_mode == "wall"
                        && decoded.state.as_ref().and_then(|s| s.name) == Some("STATE_RUNNABLE")
                    {
                        add_sample(&mut samples, 0, &stack, &context_labels, value);
                    }
                }
                EventKind::NewTlab => {
                    add_pair(&mut samples, 2, &stack, &context_labels, decoded.tlab_size);
                }
                EventKind::OutsideTlab => add_pair(
                    &mut samples,
                    4,
                    &stack,
                    &context_labels,
                    decoded.allocation_size,
                ),
                EventKind::Monitor => {
                    add_pair(&mut samples, 6, &stack, &context_labels, decoded.duration);
                }
                EventKind::Park => {
                    add_pair(&mut samples, 8, &stack, &context_labels, decoded.duration);
                }
                EventKind::Live => add_sample(&mut samples, 10, &stack, &context_labels, 1),
                EventKind::Allocation => {
                    add_pair(&mut samples, 11, &stack, &context_labels, decoded.weight);
                }
                EventKind::Malloc => {
                    add_pair(&mut samples, 13, &stack, &context_labels, decoded.size);
                }
            }
        }
    }
    if samples.is_empty() {
        return Err(ProfilesError::Decode(
            "jfr profile has no supported samples".to_string(),
        ));
    }
    Ok(build_profile(name, period, &event_mode, samples))
}

#[derive(Clone, Copy)]
enum EventKind {
    Execution,
    Wall,
    NewTlab,
    OutsideTlab,
    Monitor,
    Park,
    Live,
    Allocation,
    Malloc,
}

fn event_kind(class: &str) -> Option<EventKind> {
    match class {
        "jdk.ExecutionSample" => Some(EventKind::Execution),
        "jdk.ObjectAllocationInNewTLAB" => Some(EventKind::NewTlab),
        "jdk.ObjectAllocationOutsideTLAB" => Some(EventKind::OutsideTlab),
        "jdk.ObjectAllocationSample" => Some(EventKind::Allocation),
        "jdk.JavaMonitorEnter" => Some(EventKind::Monitor),
        "jdk.ThreadPark" => Some(EventKind::Park),
        name if name.ends_with("WallClockSample") => Some(EventKind::Wall),
        name if name.ends_with("LiveObject") => Some(EventKind::Live),
        name if name.ends_with("Malloc") => Some(EventKind::Malloc),
        _ => None,
    }
}

fn stack_frames(stack: &StackTrace<'_>) -> Option<Stack> {
    let frames = stack
        .frames
        .iter()
        .flatten()
        .filter_map(|frame| {
            let method = frame.method.as_ref()?;
            let method_name = method.name.as_ref()?.string?;
            Some(StackFrame {
                name: jfr_method_name(method.class.as_ref(), method_name),
                line: frame.line_number,
            })
        })
        .collect::<Vec<_>>();
    (!frames.is_empty()).then_some(frames)
}

fn add_sample(
    samples: &mut BTreeMap<JfrSampleKey, Vec<i64>>,
    type_index: usize,
    stack: &Stack,
    labels: &[krabka_blockstore::LabelPair],
    value: i64,
) {
    let values = samples
        .entry(JfrSampleKey {
            type_index,
            stack: stack.clone(),
            labels: labels.to_vec(),
        })
        .or_insert_with(|| vec![0; TYPES.len()]);
    values[type_index] = values[type_index].saturating_add(value);
}

fn add_pair(
    samples: &mut BTreeMap<JfrSampleKey, Vec<i64>>,
    type_index: usize,
    stack: &Stack,
    labels: &[krabka_blockstore::LabelPair],
    second: i64,
) {
    add_sample(samples, type_index, stack, labels, 1);
    add_sample(samples, type_index + 1, stack, labels, second);
}

fn build_profile(
    name: &str,
    period: i64,
    event_mode: &str,
    samples_by_stack: BTreeMap<JfrSampleKey, Vec<i64>>,
) -> PprofProfile {
    let mut strings = vec![String::new()];
    let mut string_ids = BTreeMap::new();
    string_ids.insert(String::new(), 0_i64);
    let sample_type = TYPES
        .iter()
        .map(|spec| krabka_pprof::proto::ValueType {
            r#type: intern_string(&mut strings, &mut string_ids, spec.profile_type),
            unit: intern_string(&mut strings, &mut string_ids, spec.profile_unit),
        })
        .collect();
    let mut functions = Vec::new();
    let mut function_ids = BTreeMap::new();
    let mut locations = Vec::new();
    let mut location_ids = BTreeMap::new();
    let mut samples = Vec::new();

    for sample_entry in samples_by_stack {
        let sample_key = sample_entry.0;
        let values = sample_entry.1;
        let JfrSampleKey {
            type_index,
            stack,
            labels: context_labels,
        } = sample_key;
        let mut stack_locations = Vec::new();
        for frame in stack {
            let function_id = *function_ids.entry(frame.name.clone()).or_insert_with(|| {
                let id = u64::try_from(functions.len() + 1).expect("function id fits u64");
                let name_ref = intern_string(&mut strings, &mut string_ids, &frame.name);
                functions.push(krabka_pprof::proto::Function {
                    id,
                    name: name_ref,
                    system_name: name_ref,
                    filename: 0,
                    start_line: 0,
                });
                id
            });
            let location_id = *location_ids
                .entry(LocationKey {
                    function_id,
                    line: frame.line,
                })
                .or_insert_with(|| {
                    let id = u64::try_from(locations.len() + 1).expect("location id fits u64");
                    locations.push(krabka_pprof::proto::Location {
                        id,
                        line: vec![krabka_pprof::proto::Line {
                            function_id,
                            line: i64::from(frame.line),
                            column: 0,
                        }],
                        ..Default::default()
                    });
                    id
                });
            stack_locations.push(location_id);
        }
        let spec = &TYPES[type_index];
        let metadata = [
            krabka_blockstore::LabelPair::new("__name__", spec.metric),
            krabka_blockstore::LabelPair::new("__period_type__", spec.period_type),
            krabka_blockstore::LabelPair::new("__period_unit__", spec.period_unit),
            krabka_blockstore::LabelPair::new("jfr_event", event_mode),
        ];
        let labels = metadata
            .into_iter()
            .filter(|label| !label.value.is_empty())
            .chain(context_labels)
            .map(|label| krabka_pprof::proto::Label {
                key: intern_string(&mut strings, &mut string_ids, &label.name),
                str: intern_string(&mut strings, &mut string_ids, &label.value),
                num: 0,
                num_unit: 0,
            })
            .collect();
        samples.push(krabka_pprof::proto::Sample {
            location_id: stack_locations,
            value: values,
            label: labels,
        });
    }
    let _ = intern_string(&mut strings, &mut string_ids, name);
    PprofProfile::from(krabka_pprof::proto::Profile {
        sample_type,
        sample: samples,
        location: locations,
        function: functions,
        string_table: strings,
        period,
        ..Default::default()
    })
}
