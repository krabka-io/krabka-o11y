use super::{
    Cow, Deserialize, EMPTY_STACKTRACE_ID, Frame, FunctionRec, HashMap, LineRec, LocationRec,
    MappingRec, Partition, ProfileError, RawLocation, SerdeCompat, Serialize, SymbolSource,
    TreeNode, WincodeDeserialize, WincodeSerialize, drop_go_type_parameters, remap_index,
};

/// Deduplicated symbol database for a profile block.
#[derive(Default, Clone, Debug, Serialize, Deserialize)]
pub struct SymbolDb {
    pub(crate) strings: Vec<String>,
    #[serde(skip)]
    pub(crate) string_index: HashMap<String, u32>,
    pub(crate) functions: Vec<FunctionRec>,
    #[serde(skip)]
    pub(crate) function_index: HashMap<FunctionRec, u32>,
    pub(crate) locations: Vec<LocationRec>,
    #[serde(skip)]
    pub(crate) location_index: HashMap<LocationRec, u32>,
    pub(crate) mappings: Vec<MappingRec>,
    #[serde(skip)]
    pub(crate) mapping_index: HashMap<MappingRec, u32>,
    pub(crate) partitions: HashMap<u64, Partition>,
}

impl SymbolDb {
    #[must_use]
    pub fn new() -> Self {
        let mut db = Self::default();
        db.ensure_init();
        db
    }

    pub(crate) fn ensure_init(&mut self) {
        if self.strings.is_empty() {
            self.strings.push(String::new());
            self.string_index.insert(String::new(), 0);
        }
    }

    /// # Panics
    /// Panics if decoded profile indexes reference a missing string, mapping, function, or location that validation promised was present.
    pub fn intern_string(&mut self, value: &str) -> u32 {
        self.ensure_init();
        if let Some(index) = self.string_index.get(value) {
            return *index;
        }
        let index = u32::try_from(self.strings.len()).expect("string table overflow");
        self.strings.push(value.to_string());
        self.string_index.insert(value.to_string(), index);
        index
    }

    #[must_use]
    /// # Panics
    /// Panics if decoded profile indexes reference a missing string, mapping, function, or location that validation promised was present.
    pub fn string(&self, index: u32) -> &str {
        self.strings
            .get(usize::try_from(index).expect("u32 fits usize"))
            .map_or("", String::as_str)
    }

    /// # Panics
    /// Panics if decoded profile indexes reference a missing string, mapping, function, or location that validation promised was present.
    pub fn intern_function(&mut self, function: FunctionRec) -> u32 {
        if let Some(index) = self.function_index.get(&function) {
            return *index;
        }
        let index = u32::try_from(self.functions.len()).expect("function table overflow");
        self.functions.push(function);
        self.function_index.insert(function, index);
        index
    }

    /// # Panics
    /// Panics if decoded profile indexes reference a missing string, mapping, function, or location that validation promised was present.
    pub fn intern_location(&mut self, location: LocationRec) -> u32 {
        if let Some(index) = self.location_index.get(&location) {
            return *index;
        }
        let index = u32::try_from(self.locations.len()).expect("location table overflow");
        self.locations.push(location.clone());
        self.location_index.insert(location, index);
        index
    }

    /// # Panics
    /// Panics if decoded profile indexes reference a missing string, mapping, function, or location that validation promised was present.
    pub fn intern_mapping(&mut self, mapping: MappingRec) -> u32 {
        if let Some(index) = self.mapping_index.get(&mapping) {
            return *index;
        }
        let index = u32::try_from(self.mappings.len()).expect("mapping table overflow");
        self.mappings.push(mapping);
        self.mapping_index.insert(mapping, index);
        index
    }

    /// # Panics
    /// Panics if decoded profile indexes reference a missing string, mapping, function, or location that validation promised was present.
    pub fn intern_stacktrace(&mut self, partition: u64, location_refs: &[u32]) -> u32 {
        if location_refs.is_empty() {
            return EMPTY_STACKTRACE_ID;
        }
        let part = self.partitions.entry(partition).or_default();
        let mut parent = -1;
        for location_ref in location_refs.iter().rev() {
            let location_ref = i32::try_from(*location_ref).expect("location ref fits i32");
            let key = (parent, location_ref);
            if let Some(child) = part.children.get(&key) {
                parent = i32::try_from(*child).expect("node index fits i32");
                continue;
            }
            let idx = u32::try_from(part.nodes.len()).expect("node table overflow");
            part.nodes.push(TreeNode {
                parent,
                location_ref,
            });
            part.children.insert(key, idx);
            parent = i32::try_from(idx).expect("node index fits i32");
        }
        u32::try_from(parent.max(0)).expect("leaf node index")
    }

    /// # Errors
    /// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
    /// # Panics
    /// Panics if decoded profile indexes reference a missing string, mapping, function, or location that validation promised was present.
    pub fn copy_partition_from(
        &mut self,
        source: &SymbolDb,
        source_partition: u64,
        dest_partition: u64,
    ) -> Result<(), ProfileError> {
        let Some(partition) = source.partitions.get(&source_partition) else {
            return Ok(());
        };
        if self
            .partitions
            .get(&dest_partition)
            .is_some_and(|partition| !partition.nodes.is_empty())
        {
            return Err(ProfileError::Store(format!(
                "destination symbol partition {dest_partition} is not empty"
            )));
        }

        let strings = source
            .strings
            .iter()
            .map(|value| self.intern_string(value))
            .collect::<Vec<_>>();
        let mappings = source
            .mappings
            .iter()
            .map(|mapping| {
                self.intern_mapping(MappingRec {
                    memory_start: mapping.memory_start,
                    memory_limit: mapping.memory_limit,
                    file_offset: mapping.file_offset,
                    filename: remap_index(mapping.filename, &strings),
                    build_id: remap_index(mapping.build_id, &strings),
                    symbolization: mapping.symbolization,
                })
            })
            .collect::<Vec<_>>();
        let functions = source
            .functions
            .iter()
            .map(|function| {
                self.intern_function(FunctionRec {
                    name: remap_index(function.name, &strings),
                    system_name: remap_index(function.system_name, &strings),
                    filename: remap_index(function.filename, &strings),
                    start_line: function.start_line,
                })
            })
            .collect::<Vec<_>>();
        let locations = source
            .locations
            .iter()
            .map(|location| {
                self.intern_location(LocationRec {
                    address: location.address,
                    mapping_id: remap_index(location.mapping_id, &mappings),
                    lines: location
                        .lines
                        .iter()
                        .map(|line| LineRec {
                            function_id: remap_index(line.function_id, &functions),
                            line: line.line,
                        })
                        .collect(),
                })
            })
            .collect::<Vec<_>>();

        let nodes = partition
            .nodes
            .iter()
            .map(|node| {
                let location_ref = if node.location_ref >= 0 {
                    i32::try_from(remap_index(
                        u32::try_from(node.location_ref).expect("non-negative"),
                        &locations,
                    ))
                    .expect("location index fits i32")
                } else {
                    node.location_ref
                };
                TreeNode {
                    parent: node.parent,
                    location_ref,
                }
            })
            .collect::<Vec<_>>();
        let mut copied = Partition {
            nodes,
            children: HashMap::new(),
        };
        copied.rebuild_children();
        self.partitions.insert(dest_partition, copied);
        Ok(())
    }

    #[must_use]
    /// # Panics
    /// Panics if decoded profile indexes reference a missing string, mapping, function, or location that validation promised was present.
    pub fn resolve(&self, partition: u64, stacktrace_id: u32) -> Vec<Frame> {
        if stacktrace_id == EMPTY_STACKTRACE_ID {
            return Vec::new();
        }
        let Some(part) = self.partitions.get(&partition) else {
            return Vec::new();
        };
        let mut frames = Vec::new();
        let mut current = i32::try_from(stacktrace_id).unwrap_or(-1);
        for _ in 0..part.nodes.len() {
            if current < 0 {
                break;
            }
            let Some(node) = part
                .nodes
                .get(usize::try_from(current).expect("non-negative"))
            else {
                break;
            };
            if let Some(location) = self
                .locations
                .get(usize::try_from(node.location_ref).expect("non-negative"))
            {
                for line in &location.lines {
                    let function = self
                        .functions
                        .get(usize::try_from(line.function_id).expect("u32 fits usize"));
                    frames.push(Frame {
                        function: function
                            .map_or(Cow::Borrowed(""), |func| {
                                drop_go_type_parameters(self.string(func.name))
                            })
                            .into_owned(),
                        file: function
                            .map_or("", |func| self.string(func.filename))
                            .to_string(),
                        line: line.line,
                    });
                }
            }
            current = node.parent;
        }
        frames
    }

    #[must_use]
    /// # Panics
    /// Panics if decoded profile indexes reference a missing string, mapping, function, or location that validation promised was present.
    pub fn raw_locations(&self, partition: u64, stacktrace_id: u32) -> Vec<RawLocation> {
        if stacktrace_id == EMPTY_STACKTRACE_ID {
            return Vec::new();
        }
        let Some(part) = self.partitions.get(&partition) else {
            return Vec::new();
        };
        let mut locations = Vec::new();
        let mut current = i32::try_from(stacktrace_id).unwrap_or(-1);
        for _ in 0..part.nodes.len() {
            if current < 0 {
                break;
            }
            let Some(node) = part
                .nodes
                .get(usize::try_from(current).expect("non-negative"))
            else {
                break;
            };
            if let Some(location) = self
                .locations
                .get(usize::try_from(node.location_ref).expect("non-negative"))
                && let Some(mapping) = self
                    .mappings
                    .get(usize::try_from(location.mapping_id).expect("u32 fits usize"))
            {
                locations.push(RawLocation {
                    address: location.address,
                    mapping: *mapping,
                    filename: self.string(mapping.filename).to_string(),
                    build_id: self.string(mapping.build_id).to_string(),
                });
            }
            current = node.parent;
        }
        locations
    }

    #[must_use]
    /// # Panics
    /// Panics if decoded profile indexes reference a missing string, mapping, function, or location that validation promised was present.
    pub fn encode(&self) -> Vec<u8> {
        <SerdeCompat<SymbolDb> as WincodeSerialize>::serialize(self).expect("SymbolDb serializes")
    }

    /// # Errors
    /// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
    pub fn decode(bytes: &[u8]) -> Result<Self, ProfileError> {
        let mut db = <SerdeCompat<SymbolDb> as WincodeDeserialize>::deserialize(bytes)
            .map_err(|err| ProfileError::Decode(err.to_string()))?;
        db.rebuild_indexes();
        Ok(db)
    }

    pub(crate) fn rebuild_indexes(&mut self) {
        self.string_index = self
            .strings
            .iter()
            .enumerate()
            .map(|(idx, value)| (value.clone(), u32::try_from(idx).expect("idx fits u32")))
            .collect();
        self.function_index = self
            .functions
            .iter()
            .enumerate()
            .map(|(idx, value)| (*value, u32::try_from(idx).expect("idx fits u32")))
            .collect();
        self.location_index = self
            .locations
            .iter()
            .enumerate()
            .map(|(idx, value)| (value.clone(), u32::try_from(idx).expect("idx fits u32")))
            .collect();
        self.mapping_index = self
            .mappings
            .iter()
            .enumerate()
            .map(|(idx, value)| (*value, u32::try_from(idx).expect("idx fits u32")))
            .collect();
        for partition in self.partitions.values_mut() {
            partition.rebuild_children();
        }
    }
}

impl SymbolSource for SymbolDb {
    fn resolve(&self, partition: u64, id: u32) -> Vec<Frame> {
        SymbolDb::resolve(self, partition, id)
    }
}
