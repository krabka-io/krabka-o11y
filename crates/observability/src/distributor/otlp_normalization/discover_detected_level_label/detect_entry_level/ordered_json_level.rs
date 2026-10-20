use krabka_logql::parse_json_object_entries;

use super::{Limits, level_fields};

struct PendingObject<'a> {
    depth: i64,
    entries: std::vec::IntoIter<krabka_logql::JsonObjectEntry<'a>>,
}

// ObjectEach visits object fields in physical order and preserves duplicate keys.
pub(super) fn find_json_level(line: &str, limits: &Limits) -> Option<String> {
    // Nonpositive depth limits are unlimited upstream. RawValue skips nested
    // objects without a recursion guard, so traversal must also use heap frames.
    let mut pending = vec![PendingObject {
        depth: 0,
        entries: parse_json_object_entries(line).ok()?.into_iter(),
    }];
    while let Some(object) = pending.last_mut() {
        let Some(entry) = object.entries.next() else {
            pending.pop();
            continue;
        };
        let value = entry.value.get();
        if value.starts_with('"') && level_fields(limits).any(|field| field == entry.key.as_str()) {
            // FieldDetector compares ObjectEach string bytes without JSON unescaping.
            return value
                .strip_prefix('"')
                .and_then(|value| value.strip_suffix('"'))
                .map(str::to_string);
        }
        let child_depth = object.depth + 1;
        if value.starts_with('{')
            && (limits.log_level_from_json_max_depth <= 0
                || child_depth < limits.log_level_from_json_max_depth)
            && let Ok(entries) = parse_json_object_entries(value)
        {
            pending.push(PendingObject {
                depth: child_depth,
                entries: entries.into_iter(),
            });
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::{Limits, find_json_level};

    struct DepthExpectation {
        max_depth: i64,
        expected: Option<&'static str>,
    }

    fn nested_line(depth: usize) -> String {
        format!(
            "{}{{\"level\":\"error\"}}{}",
            "{\"x\":".repeat(depth),
            "}".repeat(depth)
        )
    }

    #[test]
    fn deep_level_discovery_matches_pinned_depth_boundaries() {
        // Pinned Loki 3.7.7 getLevelUsingJSONParser returns error at depth 300
        // for limits 0/-1/301, and no match for limits 2/300.
        let line = nested_line(300);
        for case in [
            DepthExpectation {
                max_depth: 0,
                expected: Some("error"),
            },
            DepthExpectation {
                max_depth: -1,
                expected: Some("error"),
            },
            DepthExpectation {
                max_depth: 301,
                expected: Some("error"),
            },
            DepthExpectation {
                max_depth: 2,
                expected: None,
            },
            DepthExpectation {
                max_depth: 300,
                expected: None,
            },
        ] {
            let limits = Limits {
                log_level_from_json_max_depth: case.max_depth,
                ..Limits::default()
            };
            assert!(find_json_level(&line, &limits).as_deref() == case.expected);
        }
        let limits = Limits::default();
        assert!(
            find_json_level(r#"{"x":{"level":"warn"},"level":"error"}"#, &limits)
                == Some("warn".to_string())
        );
        assert!(
            find_json_level(r#"{"level":"info","level":"error"}"#, &limits)
                == Some("info".to_string())
        );
        assert!(
            find_json_level(
                r#"{"x":{"level":"ignored"},"level":"error"}"#,
                &Limits {
                    log_level_from_json_max_depth: 1,
                    ..limits
                }
            ) == Some("error".to_string())
        );
    }

    #[test]
    fn unlimited_level_discovery_does_not_use_the_thread_stack() {
        let worker = std::thread::Builder::new()
            .stack_size(128 * 1024)
            .spawn(|| {
                let line = nested_line(4096);
                [-1, 0, 4097].map(|max_depth| {
                    find_json_level(
                        &line,
                        &Limits {
                            log_level_from_json_max_depth: max_depth,
                            ..Limits::default()
                        },
                    )
                })
            })
            .expect("spawn small-stack detector thread");
        let levels = worker.join().expect("detect deep levels on a small stack");
        assert!(
            levels
                == [
                    Some("error".to_string()),
                    Some("error".to_string()),
                    Some("error".to_string())
                ]
        );
    }
}
