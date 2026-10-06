use std::sync::{Arc, Mutex};

use super::super::TemplateRuntimeValue;

/// Shared Go slice backing; views retain their range when a sibling view sorts.
#[derive(Clone, Debug)]
pub struct TemplateQueryResult {
    values: Arc<Mutex<Vec<TemplateRuntimeValue>>>,
    start: usize,
    end: usize,
}
impl From<Vec<TemplateRuntimeValue>> for TemplateQueryResult {
    fn from(values: Vec<TemplateRuntimeValue>) -> Self {
        let end = values.len();
        Self {
            values: Arc::new(Mutex::new(values)),
            start: 0,
            end,
        }
    }
}
impl PartialEq for TemplateQueryResult {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.values, &other.values)
            && self.start == other.start
            && self.end == other.end
    }
}
impl TemplateQueryResult {
    /// # Panics
    ///
    /// Panics if the shared backing lock is poisoned or the captured range is invalid.
    #[must_use]
    pub fn snapshot(&self) -> Vec<TemplateRuntimeValue> {
        self.values
            .lock()
            .expect("queryResult backing not poisoned")[self.start..self.end]
            .to_vec()
    }
    #[must_use]
    pub fn backing_address(&self) -> usize {
        // Local identities model Go backing-array pointers; views retain their
        // byte offset. They are never compared literally between processes.
        Arc::as_ptr(&self.values) as usize + self.start * 8
    }
    /// # Panics
    ///
    /// Panics if the shared backing lock is poisoned.
    #[must_use]
    pub fn get(&self, index: usize) -> Option<TemplateRuntimeValue> {
        if index >= self.len() {
            return None;
        }
        self.values
            .lock()
            .expect("queryResult backing not poisoned")
            .get(self.start + index)
            .cloned()
    }
    #[must_use]
    pub fn len(&self) -> usize {
        self.end - self.start
    }
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.start == self.end
    }
    pub(crate) fn slice(&self, start: usize, end: usize) -> Self {
        Self {
            start: self.start + start,
            end: self.start + end,
            ..self.clone()
        }
    }
    pub(crate) fn replace_prefix(&self, replacement: &[TemplateRuntimeValue]) -> Self {
        let mut values = self.values.lock().expect("slice backing not poisoned");
        for (index, value) in replacement.iter().cloned().enumerate() {
            if let Some(existing) = values.get_mut(index) {
                *existing = value;
            } else {
                values.push(value);
            }
        }
        Self {
            start: 0,
            end: replacement.len(),
            values: Arc::clone(&self.values),
        }
    }
    pub(crate) fn sort_by_label(&self, label: &[u8]) -> Result<(), String> {
        let mut values = self
            .values
            .lock()
            .map_err(|_| "queryResult backing poisoned")?;
        for value in &values[self.start..self.end] {
            super::sample_label(value, label)?;
        }
        values[self.start..self.end].sort_by_cached_key(|value| {
            super::sample_label(value, label).expect("validated sample")
        });
        Ok(())
    }
}
