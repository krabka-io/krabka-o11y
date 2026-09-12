use jfrs::reader::types::builtin::{StackTrace, ThreadState};

use super::{
    BTreeMap, Cursor, Deserialize, IngestQuery, JfrLabels, PprofProfile, ProfilesError,
    intern_string, jfr_method_name,
};

const TYPES: &[(&str, &str, &str, &str, &str)] = &[
    ("cpu", "nanoseconds", "process_cpu", "cpu", "nanoseconds"),
    ("wall", "nanoseconds", "wall", "wall", "nanoseconds"),
    (
        "alloc_in_new_tlab_objects",
        "count",
        "memory",
        "space",
        "bytes",
    ),
    (
        "alloc_in_new_tlab_bytes",
        "bytes",
        "memory",
        "space",
        "bytes",
    ),
    (
        "alloc_outside_tlab_objects",
        "count",
        "memory",
        "space",
        "bytes",
    ),
    (
        "alloc_outside_tlab_bytes",
        "bytes",
        "memory",
        "space",
        "bytes",
    ),
    ("contentions", "count", "mutex", "mutex", "count"),
    ("delay", "nanoseconds", "mutex", "mutex", "count"),
    ("contentions", "count", "block", "block", "count"),
    ("delay", "nanoseconds", "block", "block", "count"),
    ("live", "count", "memory", "objects", "count"),
    ("alloc_sample_objects", "count", "memory", "space", "bytes"),
    ("alloc_sample_bytes", "bytes", "memory", "space", "bytes"),
    ("malloc_objects", "count", "memory", "", ""),
    ("malloc_bytes", "bytes", "memory", "", ""),
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

type Stack = Vec<(String, i32)>;
type SampleKey = (usize, Stack, Vec<(String, String)>);

pub(crate) fn binary_jfr_to_pprof(
    name: &str,
    raw: &[u8],
    query: &IngestQuery,
    labels: &JfrLabels,
) -> Result<PprofProfile, ProfilesError> {
    let period = (1_000_000_000_i64 / i64::from(query.sample_rate)).max(1);
    let mut event_mode = query.jfr_event.clone();
    let mut samples = BTreeMap::<SampleKey, Vec<i64>>::new();
    let mut reader = jfrs::reader::JfrReader::new(Cursor::new(raw.to_vec()));

    for chunk in reader.chunks() {
        let (mut chunk_reader, chunk) =
            chunk.map_err(|err| ProfilesError::Decode(format!("jfr chunk decode: {err}")))?;
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
            Some((
                jfr_method_name(method.class.as_ref(), method_name),
                frame.line_number,
            ))
        })
        .collect::<Vec<_>>();
    (!frames.is_empty()).then_some(frames)
}

fn add_sample(
    samples: &mut BTreeMap<SampleKey, Vec<i64>>,
    type_index: usize,
    stack: &Stack,
    labels: &[(String, String)],
    value: i64,
) {
    let values = samples
        .entry((type_index, stack.clone(), labels.to_vec()))
        .or_insert_with(|| vec![0; TYPES.len()]);
    values[type_index] = values[type_index].saturating_add(value);
}

fn add_pair(
    samples: &mut BTreeMap<SampleKey, Vec<i64>>,
    type_index: usize,
    stack: &Stack,
    labels: &[(String, String)],
    second: i64,
) {
    add_sample(samples, type_index, stack, labels, 1);
    add_sample(samples, type_index + 1, stack, labels, second);
}

fn build_profile(
    name: &str,
    period: i64,
    event_mode: &str,
    samples_by_stack: BTreeMap<SampleKey, Vec<i64>>,
) -> PprofProfile {
    let mut strings = vec![String::new()];
    let mut string_ids = BTreeMap::from([(String::new(), 0_i64)]);
    let sample_type = TYPES
        .iter()
        .map(
            |(sample_type, unit, _, _, _)| krabka_pprof::proto::ValueType {
                r#type: intern_string(&mut strings, &mut string_ids, sample_type),
                unit: intern_string(&mut strings, &mut string_ids, unit),
            },
        )
        .collect();
    let mut functions = Vec::new();
    let mut function_ids = BTreeMap::new();
    let mut locations = Vec::new();
    let mut location_ids = BTreeMap::new();
    let mut samples = Vec::new();

    for ((type_index, stack, context_labels), values) in samples_by_stack {
        let mut stack_locations = Vec::new();
        for (frame, line) in stack.into_iter().rev() {
            let function_id = *function_ids.entry(frame.clone()).or_insert_with(|| {
                let id = u64::try_from(functions.len() + 1).expect("function id fits u64");
                let name_ref = intern_string(&mut strings, &mut string_ids, &frame);
                functions.push(krabka_pprof::proto::Function {
                    id,
                    name: name_ref,
                    system_name: name_ref,
                    filename: 0,
                    start_line: 0,
                });
                id
            });
            let location_id = *location_ids.entry((function_id, line)).or_insert_with(|| {
                let id = u64::try_from(locations.len() + 1).expect("location id fits u64");
                locations.push(krabka_pprof::proto::Location {
                    id,
                    line: vec![krabka_pprof::proto::Line {
                        function_id,
                        line: i64::from(line),
                        column: 0,
                    }],
                    ..Default::default()
                });
                id
            });
            stack_locations.push(location_id);
        }
        let (_, _, metric, period_type, period_unit) = TYPES[type_index];
        let metadata = [
            ("__name__", metric),
            ("__period_type__", period_type),
            ("__period_unit__", period_unit),
            ("jfr_event", event_mode),
        ];
        let labels = metadata
            .into_iter()
            .filter(|(_, value)| !value.is_empty())
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .chain(context_labels)
            .map(|(key, value)| krabka_pprof::proto::Label {
                key: intern_string(&mut strings, &mut string_ids, &key),
                str: intern_string(&mut strings, &mut string_ids, &value),
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
