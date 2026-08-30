use super::{
    Function, HashMap, Line, Location, PprofProfile, Profile, ProfileType, Sample, ValueType,
    intern_string,
};

pub(crate) struct PprofBuilder {
    pub(crate) profile: Profile,
    pub(crate) strings: HashMap<String, i64>,
    pub(crate) locations: HashMap<String, u64>,
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
            locations: HashMap::new(),
        }
    }

    pub(crate) fn add_sample(&mut self, root_to_leaf: &[String], value: i64) {
        let location_id = root_to_leaf
            .iter()
            .rev()
            .map(|name| self.location_id(name))
            .collect();
        self.profile.sample.push(Sample {
            location_id,
            value: vec![value],
            label: Vec::new(),
        });
    }

    pub(crate) fn location_id(&mut self, name: &str) -> u64 {
        if let Some(id) = self.locations.get(name) {
            return *id;
        }
        let name_ref = intern_string(&mut self.profile.string_table, &mut self.strings, name);
        let id = u64::try_from(self.profile.function.len() + 1).expect("function id fits u64");
        self.profile.function.push(Function {
            id,
            name: name_ref,
            system_name: name_ref,
            filename: 0,
            start_line: 0,
        });
        self.profile.location.push(Location {
            id,
            mapping_id: 0,
            address: 0,
            line: vec![Line {
                function_id: id,
                line: 0,
                column: 0,
            }],
            is_folded: false,
        });
        self.locations.insert(name.to_string(), id);
        id
    }

    pub(crate) fn finish(self) -> PprofProfile {
        self.profile.into()
    }
}
