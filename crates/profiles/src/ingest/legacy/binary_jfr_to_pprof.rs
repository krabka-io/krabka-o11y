use super::{BTreeMap, Cursor, PprofProfile, ProfilesError, jfr_method_name, stacks_to_pprof};

pub(crate) fn binary_jfr_to_pprof(name: &str, raw: &[u8]) -> Result<PprofProfile, ProfilesError> {
    let mut reader = jfrs::reader::JfrReader::new(Cursor::new(raw.to_vec()));
    let mut stacks = BTreeMap::<Vec<(String, i32)>, i64>::new();
    for chunk in reader.chunks() {
        let (mut chunk_reader, chunk) =
            chunk.map_err(|err| ProfilesError::Decode(format!("jfr chunk decode: {err}")))?;
        for event in chunk_reader.events(&chunk) {
            let event =
                event.map_err(|err| ProfilesError::Decode(format!("jfr event decode: {err}")))?;
            if event.class.name() != "jdk.ExecutionSample" {
                continue;
            }
            let sample: jfrs::reader::types::jdk::ExecutionSample<'_> =
                jfrs::reader::from_event(&event).map_err(|err| {
                    ProfilesError::Decode(format!("jfr execution sample decode: {err}"))
                })?;
            let Some(stack) = sample.stack_trace else {
                continue;
            };
            let frames = stack
                .frames
                .into_iter()
                .flatten()
                .filter_map(|frame| {
                    let method = frame.method?;
                    let method_name = method.name.and_then(|name| name.string)?;
                    Some((
                        jfr_method_name(method.class, method_name),
                        frame.line_number,
                    ))
                })
                .collect::<Vec<_>>();
            if !frames.is_empty() {
                *stacks.entry(frames).or_default() += 1;
            }
        }
    }
    if stacks.is_empty() {
        return Err(ProfilesError::Decode(
            "jfr profile has no execution samples".to_string(),
        ));
    }
    Ok(stacks_to_pprof(name, "wall", "nanoseconds", stacks))
}
