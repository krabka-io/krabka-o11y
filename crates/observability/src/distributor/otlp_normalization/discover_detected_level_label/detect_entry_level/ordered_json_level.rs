use krabka_logql::parse_json_object_entries;

use super::{Limits, level_fields};

// ObjectEach visits object fields in physical order and preserves duplicate keys.
pub(super) fn find_json_level(line: &str, limits: &Limits) -> Option<String> {
    // Nonpositive depth limits are unlimited upstream. RawValue skips nested
    // objects without a recursion guard, so traversal must also use heap frames.
    let mut pending = vec![(0, parse_json_object_entries(line).ok()?.into_iter())];
    while let Some((depth, entries)) = pending.last_mut() {
        let Some((name, value)) = entries.next() else {
            pending.pop();
            continue;
        };
        let value = value.get();
        if value.starts_with('"') && level_fields(limits).any(|field| field == name) {
            // FieldDetector compares ObjectEach string bytes without JSON unescaping.
            return value
                .strip_prefix('"')
                .and_then(|value| value.strip_suffix('"'))
                .map(str::to_string);
        }
        let child_depth = *depth + 1;
        if value.starts_with('{')
            && (limits.log_level_from_json_max_depth <= 0
                || child_depth < limits.log_level_from_json_max_depth)
            && let Ok(entries) = parse_json_object_entries(value)
        {
            pending.push((child_depth, entries.into_iter()));
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::{Limits, find_json_level};

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
        for (max_depth, expected) in [
            (0, Some("error")),
            (-1, Some("error")),
            (301, Some("error")),
            (2, None),
            (300, None),
        ] {
            let limits = Limits {
                log_level_from_json_max_depth: max_depth,
                ..Limits::default()
            };
            assert!(find_json_level(&line, &limits).as_deref() == expected);
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
