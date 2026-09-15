use super::{
    Frame, Function, HashMap, Line, Location, Mapping, PprofProfile, Profile, ProfileType,
    ResolvedFunction, ResolvedLocation, ResolvedMapping, Sample, ValueType, intern_string,
};

pub(crate) struct PprofBuilder {
    pub(crate) profile: Profile,
    pub(crate) strings: HashMap<String, i64>,
    pub(crate) functions: HashMap<ResolvedFunction, u64>,
    pub(crate) locations: HashMap<ResolvedLocation, u64>,
    pub(crate) mappings: HashMap<ResolvedMapping, u64>,
}

impl PprofBuilder {
    pub(crate) fn new(profile_type: &ProfileType) -> Self {
        let mut profile = Profile {
            string_table: vec![String::new()],
            ..Default::default()
        };
        let mut strings = HashMap::from([(String::new(), 0)]);
        let sample_type = intern_string(
            &mut profile.string_table,
            &mut strings,
            &profile_type.sample_type,
        );
        let sample_unit = intern_string(
            &mut profile.string_table,
            &mut strings,
            &profile_type.sample_unit,
        );
        let period_type = intern_string(
            &mut profile.string_table,
            &mut strings,
            &profile_type.period_type,
        );
        let period_unit = intern_string(
            &mut profile.string_table,
            &mut strings,
            &profile_type.period_unit,
        );
        profile.sample_type.push(ValueType {
            r#type: sample_type,
            unit: sample_unit,
        });
        profile.period_type = Some(ValueType {
            r#type: period_type,
            unit: period_unit,
        });
        profile.default_sample_type = sample_type;
        Self {
            profile,
            strings,
            functions: HashMap::new(),
            locations: HashMap::new(),
            mappings: HashMap::new(),
        }
    }

    pub(crate) fn add_sample(&mut self, root_to_leaf: &[String], value: i64) {
        let locations = root_to_leaf
            .iter()
            .rev()
            .map(|name| {
                ResolvedLocation::from(Frame {
                    function: name.clone(),
                    file: String::new(),
                    line: 0,
                })
            })
            .collect::<Vec<_>>();
        self.add_resolved_sample(&locations, value);
    }

    pub(crate) fn add_resolved_sample(&mut self, leaf_to_root: &[ResolvedLocation], value: i64) {
        let location_id = leaf_to_root
            .iter()
            .map(|location| self.location_id(location))
            .collect();
        self.profile.sample.push(Sample {
            location_id,
            value: vec![value],
            label: Vec::new(),
        });
    }

    pub(crate) fn location_id(&mut self, location: &ResolvedLocation) -> u64 {
        if let Some(id) = self.locations.get(location) {
            return *id;
        }
        let mapping_id = location
            .mapping
            .as_ref()
            .map_or(0, |mapping| self.mapping_id(mapping));
        let lines = location
            .lines
            .iter()
            .map(|line| Line {
                function_id: self.function_id(&line.function),
                line: line.line,
                column: 0,
            })
            .collect();
        let id = u64::try_from(self.profile.location.len() + 1).expect("location id fits u64");
        self.profile.location.push(Location {
            id,
            mapping_id,
            address: location.address,
            line: lines,
            is_folded: false,
        });
        self.locations.insert(location.clone(), id);
        id
    }

    fn function_id(&mut self, function: &ResolvedFunction) -> u64 {
        if let Some(id) = self.functions.get(function) {
            return *id;
        }
        let name = intern_string(
            &mut self.profile.string_table,
            &mut self.strings,
            &function.name,
        );
        let system_name = intern_string(
            &mut self.profile.string_table,
            &mut self.strings,
            &function.system_name,
        );
        let filename = intern_string(
            &mut self.profile.string_table,
            &mut self.strings,
            &function.filename,
        );
        let id = u64::try_from(self.profile.function.len() + 1).expect("function id fits u64");
        self.profile.function.push(Function {
            id,
            name,
            system_name,
            filename,
            start_line: function.start_line,
        });
        self.functions.insert(function.clone(), id);
        id
    }

    fn mapping_id(&mut self, mapping: &ResolvedMapping) -> u64 {
        if let Some(id) = self.mappings.get(mapping) {
            return *id;
        }
        let filename = intern_string(
            &mut self.profile.string_table,
            &mut self.strings,
            &mapping.filename,
        );
        let build_id = intern_string(
            &mut self.profile.string_table,
            &mut self.strings,
            &mapping.build_id,
        );
        let id = u64::try_from(self.profile.mapping.len() + 1).expect("mapping id fits u64");
        self.profile.mapping.push(Mapping {
            id,
            memory_start: mapping.memory_start,
            memory_limit: mapping.memory_limit,
            file_offset: mapping.file_offset,
            filename,
            build_id,
            symbolization: crate::proto::MappingSymbolization::from_parts((
                mapping.has_functions,
                mapping.has_filenames,
                mapping.has_line_numbers,
                mapping.has_inline_frames,
            )),
        });
        self.mappings.insert(mapping.clone(), id);
        id
    }

    pub(crate) fn finish(self) -> PprofProfile {
        self.profile.into()
    }
}
