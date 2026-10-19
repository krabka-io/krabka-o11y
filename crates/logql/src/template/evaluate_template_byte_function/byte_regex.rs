use std::{
    collections::VecDeque,
    sync::{Arc, Mutex, OnceLock},
};

use regex::Regex;

use super::runes;

const REGEX_CACHE_CAPACITY: usize = 16;

type CompiledRegex = Result<Arc<Regex>, String>;

#[derive(Debug)]
pub(crate) struct CachedRegex {
    pattern: Vec<u8>,
    compiled: CompiledRegex,
}

// Each command owns a bounded cache. Clones share it, but cache state does
// not change template equality. Compile only when the command executes.
#[derive(Clone, Debug)]
pub(crate) enum TemplateRegexCache {
    Constant(Arc<OnceLock<CompiledRegex>>),
    Dynamic(Arc<Mutex<VecDeque<CachedRegex>>>),
}

impl Default for TemplateRegexCache {
    fn default() -> Self {
        Self::Dynamic(Arc::default())
    }
}

impl PartialEq for TemplateRegexCache {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for TemplateRegexCache {}

impl TemplateRegexCache {
    pub(crate) fn constant() -> Self {
        Self::Constant(Arc::default())
    }

    pub(crate) fn get(&self, pattern: &[u8]) -> CompiledRegex {
        let dynamic = match self {
            Self::Constant(constant) => return constant.get_or_init(|| compile(pattern)).clone(),
            Self::Dynamic(dynamic) => dynamic,
        };
        let mut entries = dynamic.lock().expect("template regex cache lock poisoned");
        if let Some(entry) = entries.iter().find(|entry| entry.pattern == pattern) {
            return entry.compiled.clone();
        }
        let compiled = compile(pattern);
        if entries.len() == REGEX_CACHE_CAPACITY {
            entries.pop_front();
        }
        entries.push_back(CachedRegex {
            pattern: pattern.to_vec(),
            compiled: compiled.clone(),
        });
        compiled
    }
}

fn compile(pattern: &[u8]) -> CompiledRegex {
    std::str::from_utf8(pattern)
        .map_err(|_| "invalid UTF-8 in regexp".to_string())
        .and_then(|pattern| {
            Regex::new(pattern)
                .map(Arc::new)
                .map_err(|error| error.to_string())
        })
}

pub(super) struct RegexReplacement {
    pub value: Vec<u8>,
    pub matches: usize,
}

pub(super) fn regex_apply(
    name: &str,
    pattern: &[u8],
    input: &[u8],
    replacement: &[u8],
) -> Option<RegexReplacement> {
    let pattern = std::str::from_utf8(pattern).ok()?;
    let regex = Regex::new(pattern).ok()?;
    Some(regex_apply_compiled(name, &regex, input, replacement))
}

pub(super) fn regex_apply_compiled(
    name: &str,
    regex: &Regex,
    input: &[u8],
    replacement: &[u8],
) -> RegexReplacement {
    // Go's regex reader decodes each invalid byte as RuneError while retaining
    // original byte offsets for unmatched text and captures.
    let mut normalized = String::new();
    let mut offsets = vec![0];
    for rune in runes(input) {
        let before = normalized.len();
        normalized.push(rune.character);
        offsets.extend(std::iter::repeat_n(
            rune.start,
            normalized.len() - before - 1,
        ));
        offsets.push(rune.start + rune.length);
    }
    let mut output = Vec::new();
    let mut end = 0;
    let mut count = 0;
    for captures in regex.captures_iter(&normalized) {
        let found = captures
            .get(0)
            .expect("regex captures include entire match");
        let start = offsets[found.start()];
        let next = offsets[found.end()];
        output.extend(&input[end..start]);
        if name == "regexReplaceAllLiteral" {
            output.extend(replacement);
        } else {
            expand(&mut output, replacement, &captures, input, &offsets);
        }
        end = next;
        count += 1;
    }
    output.extend(&input[end..]);
    RegexReplacement {
        value: output,
        matches: count,
    }
}

fn expand(
    output: &mut Vec<u8>,
    replacement: &[u8],
    captures: &regex::Captures<'_>,
    input: &[u8],
    offsets: &[usize],
) {
    let mut position = 0;
    while position < replacement.len() {
        if replacement[position] != b'$' {
            output.push(replacement[position]);
            position += 1;
            continue;
        }
        position += 1;
        if replacement.get(position) == Some(&b'$') {
            output.push(b'$');
            position += 1;
            continue;
        }
        let braced = replacement.get(position) == Some(&b'{');
        let start = position + usize::from(braced);
        let mut end = start;
        while replacement
            .get(end)
            .is_some_and(|byte| byte.is_ascii_alphanumeric() || *byte == b'_')
        {
            end += 1;
        }
        if end == start || (braced && replacement.get(end) != Some(&b'}')) {
            output.push(b'$');
            continue;
        }
        let name = std::str::from_utf8(&replacement[start..end]).expect("ASCII replacement name");
        let found = if name.bytes().all(|byte| byte.is_ascii_digit()) {
            if name.len() > 1 && name.starts_with('0') {
                None
            } else {
                name.parse::<usize>()
                    .ok()
                    .and_then(|index| captures.get(index))
            }
        } else {
            captures.name(name)
        };
        if let Some(found) = found {
            output.extend(&input[offsets[found.start()]..offsets[found.end()]]);
        }
        position = end + usize::from(braced);
    }
}

#[cfg(test)]
mod cache_tests {
    use assert2::assert;

    use super::{REGEX_CACHE_CAPACITY, TemplateRegexCache};

    #[test]
    fn reuses_patterns_across_clones_and_bounds_dynamic_patterns() {
        let cache = TemplateRegexCache::default();
        let first = cache.get("(a+)".as_bytes()).unwrap();
        assert!(std::sync::Arc::ptr_eq(
            &first,
            &cache.clone().get(b"(a+)").unwrap()
        ));
        for index in 0..REGEX_CACHE_CAPACITY {
            cache.get(format!("pattern-{index}").as_bytes()).unwrap();
        }
        assert!(!std::sync::Arc::ptr_eq(
            &first,
            &cache.get(b"(a+)").unwrap()
        ));
        assert!(cache == TemplateRegexCache::default());
    }

    #[test]
    fn constant_patterns_initialize_once_across_clones() {
        let cache = TemplateRegexCache::constant();
        let first = cache.get(b"a+").unwrap();
        assert!(std::sync::Arc::ptr_eq(&first, &cache.get(b"a+").unwrap()));
        assert!(std::sync::Arc::ptr_eq(
            &first,
            &cache.clone().get(b"a+").unwrap()
        ));
    }

    #[test]
    fn invalid_patterns_keep_their_errors() {
        let cache = TemplateRegexCache::default();
        for pattern in [b"[".as_slice(), b"\xff"] {
            let first = cache.get(pattern).unwrap_err();
            assert!(cache.get(pattern).unwrap_err() == first);
        }
        assert!(cache.get(b"\xff").unwrap_err() == "invalid UTF-8 in regexp");
    }
}
