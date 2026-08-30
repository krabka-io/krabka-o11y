use super::*;

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct DetectedFieldStats {
    pub(crate) ty: DetectedFieldType,
    pub(crate) values: BTreeSet<String>,
    pub(crate) parsers: BTreeSet<&'static str>,
}

impl DetectedFieldStats {
    pub(crate) fn new(ty: DetectedFieldType, value: String, parser: &'static str) -> Self {
        Self {
            ty,
            values: BTreeSet::from([value]),
            parsers: BTreeSet::from([parser]),
        }
    }

    pub(crate) fn new_generated(ty: DetectedFieldType, value: String) -> Self {
        Self {
            ty,
            values: BTreeSet::from([value]),
            parsers: BTreeSet::new(),
        }
    }

    pub(crate) fn add(&mut self, ty: DetectedFieldType, value: String, parser: &'static str) {
        self.ty = self.ty.merge(ty);
        self.values.insert(value);
        self.parsers.insert(parser);
    }

    pub(crate) fn add_generated(&mut self, ty: DetectedFieldType, value: String) {
        self.ty = self.ty.merge(ty);
        self.values.insert(value);
    }

    pub(crate) fn parsers_json(self) -> Value {
        if self.parsers.is_empty() {
            Value::Null
        } else {
            json!(self.parsers.into_iter().collect::<Vec<_>>())
        }
    }
}
