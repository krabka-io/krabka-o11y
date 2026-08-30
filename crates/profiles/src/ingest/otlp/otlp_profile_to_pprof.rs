use super::*;

pub(crate) fn otlp_profile_to_pprof(
    profile: &pb::otlp_profiles::Profile,
    dict: &pb::otlp_profiles::ProfilesDictionary,
) -> Result<PprofProfile, ProfilesError> {
    let mut pprof = krabka_pprof::proto::Profile {
        string_table: string_table(dict),
        mapping: dict
            .mapping_table
            .iter()
            .enumerate()
            .map(|(idx, mapping)| krabka_pprof::proto::Mapping {
                id: u64::try_from(idx + 1).unwrap_or(u64::MAX),
                memory_start: mapping.memory_start,
                memory_limit: mapping.memory_limit,
                file_offset: mapping.file_offset,
                filename: i64::from(mapping.filename_strindex),
                ..Default::default()
            })
            .collect(),
        function: dict
            .function_table
            .iter()
            .enumerate()
            .map(|(idx, function)| krabka_pprof::proto::Function {
                id: u64::try_from(idx + 1).unwrap_or(u64::MAX),
                name: i64::from(function.name_strindex),
                system_name: i64::from(function.system_name_strindex),
                filename: i64::from(function.filename_strindex),
                start_line: function.start_line,
            })
            .collect(),
        location: dict
            .location_table
            .iter()
            .enumerate()
            .map(|(idx, location)| krabka_pprof::proto::Location {
                id: u64::try_from(idx + 1).unwrap_or(u64::MAX),
                mapping_id: table_ref(location.mapping_index, dict.mapping_table.len()),
                address: location.address,
                line: location
                    .lines
                    .iter()
                    .map(|line| krabka_pprof::proto::Line {
                        function_id: table_ref(line.function_index, dict.function_table.len()),
                        line: line.line,
                        column: line.column,
                    })
                    .collect(),
                ..Default::default()
            })
            .collect(),
        time_nanos: i64::try_from(profile.time_unix_nano)
            .map_err(|_| ProfilesError::Invalid("OTLP profile time overflows i64".to_string()))?,
        duration_nanos: i64::try_from(profile.duration_nano).map_err(|_| {
            ProfilesError::Invalid("OTLP profile duration overflows i64".to_string())
        })?,
        period: profile.period,
        ..Default::default()
    };

    if let Some(sample_type) = &profile.sample_type {
        pprof.sample_type.push(value_type(*sample_type));
    }
    if let Some(period_type) = &profile.period_type {
        pprof.period_type = Some(value_type(*period_type));
    }

    for sample in &profile.samples {
        let stack = usize::try_from(sample.stack_index)
            .ok()
            .and_then(|idx| dict.stack_table.get(idx))
            .ok_or_else(|| ProfilesError::Invalid("OTLP sample references missing stack".into()))?;
        let location_id = stack
            .location_indices
            .iter()
            .map(|idx| {
                table_ref_checked(
                    *idx,
                    dict.location_table.len(),
                    "OTLP stack references missing location",
                )
            })
            .collect::<Result<Vec<_>, ProfilesError>>()?;
        pprof.sample.push(krabka_pprof::proto::Sample {
            location_id,
            value: sample.values.clone(),
            label: sample_labels(sample, dict, &mut pprof.string_table)?,
        });
    }

    Ok(PprofProfile::from(pprof))
}
