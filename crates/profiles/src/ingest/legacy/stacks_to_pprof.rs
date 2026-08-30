use super::*;

pub(crate) fn stacks_to_pprof(
    name: &str,
    sample_type: &str,
    sample_unit: &str,
    stacks: BTreeMap<Vec<(String, i32)>, i64>,
) -> PprofProfile {
    let mut string_ids = BTreeMap::from([
        (String::new(), 0_i64),
        (sample_type.to_string(), 1_i64),
        (sample_unit.to_string(), 2_i64),
    ]);
    let mut strings = vec![
        String::new(),
        sample_type.to_string(),
        sample_unit.to_string(),
    ];
    let mut function_ids = BTreeMap::new();
    let mut functions = Vec::new();
    let mut locations = Vec::new();
    let mut samples = Vec::new();

    for (stack, value) in stacks {
        let mut location_ids = Vec::new();
        for (frame, line) in stack.into_iter().rev() {
            let function_id = if let Some(id) = function_ids.get(&frame) {
                *id
            } else {
                let name_ref = intern_string(&mut strings, &mut string_ids, &frame);
                let id = i64::try_from(functions.len() + 1).expect("function id fits i64");
                functions.push(krabka_pprof::proto::Function {
                    id: u64::try_from(id).expect("positive id fits u64"),
                    name: name_ref,
                    system_name: name_ref,
                    filename: 0,
                    start_line: 0,
                });
                locations.push(krabka_pprof::proto::Location {
                    id: u64::try_from(id).expect("positive id fits u64"),
                    line: vec![krabka_pprof::proto::Line {
                        function_id: u64::try_from(id).expect("positive id fits u64"),
                        line: i64::from(line),
                        column: 0,
                    }],
                    ..Default::default()
                });
                function_ids.insert(frame, id);
                id
            };
            location_ids.push(u64::try_from(function_id).expect("positive id fits u64"));
        }
        samples.push(krabka_pprof::proto::Sample {
            location_id: location_ids,
            value: vec![value],
            label: Vec::new(),
        });
    }

    let _ = intern_string(&mut strings, &mut string_ids, name);
    PprofProfile::from(krabka_pprof::proto::Profile {
        sample_type: vec![krabka_pprof::proto::ValueType { r#type: 1, unit: 2 }],
        sample: samples,
        location: locations,
        function: functions,
        string_table: strings,
        period_type: Some(krabka_pprof::proto::ValueType { r#type: 1, unit: 2 }),
        ..Default::default()
    })
}
