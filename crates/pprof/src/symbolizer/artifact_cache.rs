use std::{
    collections::VecDeque,
    time::{Duration, Instant},
};

use super::{HashMap, ObjectSymbolResolver};

struct ArtifactCacheEntry {
    resolver: Option<ObjectSymbolResolver>,
    inserted: Instant,
}

pub(crate) struct ArtifactCache {
    entries: HashMap<String, ArtifactCacheEntry>,
    order: VecDeque<String>,
    bytes: usize,
    max_bytes: usize,
    negative_ttl: Duration,
}

impl ArtifactCache {
    pub(crate) fn new(max_bytes: usize, negative_ttl: Duration) -> Self {
        Self {
            entries: HashMap::new(),
            order: VecDeque::new(),
            bytes: 0,
            max_bytes,
            negative_ttl,
        }
    }

    // The outer option is a cache miss; the inner one is a cached failed lookup.
    #[allow(clippy::option_option)]
    pub(crate) fn get(&mut self, key: &str) -> Option<Option<ObjectSymbolResolver>> {
        let expired = self.entries.get(key).is_some_and(|entry| {
            entry.resolver.is_none() && entry.inserted.elapsed() >= self.negative_ttl
        });
        if expired {
            self.remove(key);
            return None;
        }
        let value = self.entries.get(key)?.resolver.clone();
        self.order.retain(|candidate| candidate != key);
        self.order.push_back(key.to_string());
        Some(value)
    }

    pub(crate) fn insert(&mut self, key: String, resolver: Option<ObjectSymbolResolver>) {
        self.remove(&key);
        let size = key.len()
            + resolver
                .as_ref()
                .map_or(0, ObjectSymbolResolver::artifact_size);
        if size > self.max_bytes {
            return;
        }
        while self.bytes.saturating_add(size) > self.max_bytes {
            let Some(oldest) = self.order.pop_front() else {
                break;
            };
            self.remove_entry(&oldest);
        }
        self.bytes = self.bytes.saturating_add(size);
        self.order.push_back(key.clone());
        self.entries.insert(
            key,
            ArtifactCacheEntry {
                resolver,
                inserted: Instant::now(),
            },
        );
    }

    fn remove(&mut self, key: &str) {
        self.order.retain(|candidate| candidate != key);
        self.remove_entry(key);
    }

    fn remove_entry(&mut self, key: &str) {
        if let Some(entry) = self.entries.remove(key) {
            let size = key.len()
                + entry
                    .resolver
                    .as_ref()
                    .map_or(0, ObjectSymbolResolver::artifact_size);
            self.bytes = self.bytes.saturating_sub(size);
        }
    }
}
