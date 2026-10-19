use std::collections::BTreeMap;

use assert2::assert;
use krabka_logql::LineFormat;

#[test]
fn reused_dynamic_patterns_preserve_captures_and_literal_replacements() {
    let template = r#"{{ count .pattern .input }}|{{ regexReplaceAll .pattern .input "${word}:$2:$$" }}|{{ regexReplaceAllLiteral .pattern .input "${word}:$2:$$" }}"#;
    let format = LineFormat::new(template).unwrap();
    for _ in 0..2 {
        for (pattern, input, expected) in [
            ("(?P<word>a)(b+)", "abb", "1|a:bb:$|${word}:$2:$$"),
            ("(?P<word>x)(y+)", "xy", "1|x:y:$|${word}:$2:$$"),
            ("(?P<word>a)(b+)", "abb", "1|a:bb:$|${word}:$2:$$"),
        ] {
            let fields = BTreeMap::from([
                ("pattern".into(), pattern.into()),
                ("input".into(), input.into()),
            ]);
            assert!(format.render("raw", &fields) == expected);
        }
    }
    assert!(format == LineFormat::new(template).unwrap());
    assert!(
        format.clone().render(
            "raw",
            &BTreeMap::from([("pattern".into(), "z".into()), ("input".into(), "z".into())])
        ) == "1|::$|${word}:$2:$$"
    );
}

#[test]
fn unused_invalid_patterns_stay_lazy_after_repeated_renders() {
    for template in [
        r#"{{ if .skip }}ok{{ else }}{{ count "[" "a" }}{{ end }}"#,
        r#"{{ if .skip }}ok{{ else }}{{ regexReplaceAll "[" "a" "b" }}{{ end }}"#,
        r#"{{ if .skip }}ok{{ else }}{{ regexReplaceAllLiteral "[" "a" "b" }}{{ end }}"#,
        r#"{{ or .skip (count "[" "a") }}"#,
        r#"{{ or .skip (count "\xff" "a") }}"#,
    ] {
        let format = LineFormat::new(template).unwrap();
        let fields = BTreeMap::from([("skip".into(), "ok".into())]);
        for _ in 0..2 {
            assert!(format.render("raw", &fields) == "ok");
            assert!(format.render("raw", &BTreeMap::new()).is_empty());
        }
    }
}

#[test]
fn cached_patterns_preserve_invalid_input_bytes() {
    let format = LineFormat::new(r#"{{ b64enc (regexReplaceAll "(.)(.)" "\xff\xfe" "$2$1") }}|{{ count "." "\xff\xfe" }}|{{ b64enc (regexReplaceAllLiteral "." "\xff\xfe" "\xfe") }}"#).unwrap();
    for _ in 0..2 {
        assert!(format.render("raw", &BTreeMap::new()) == "/v8=|2|/v4=");
    }
}

#[test]
fn invalid_dynamic_patterns_do_not_poison_later_lines() {
    let format = LineFormat::new("{{ count .pattern __line__ }}").unwrap();
    for pattern in ["a", "[", "b", "[", "a"] {
        let fields = BTreeMap::from([("pattern".into(), pattern.into())]);
        let expected = if pattern == "[" { "" } else { "1" };
        assert!(format.render("ab", &fields) == expected);
    }
}

#[test]
fn label_format_reuses_patterns_and_reports_dynamic_errors() {
    let query = krabka_logql::parse_query(
        r#"{app="api"} | label_format total=`{{ count .pattern __line__ }}`"#,
    )
    .unwrap();
    let labels = BTreeMap::from([("app".into(), "api".into())]);
    for pattern in ["a", "[", "b", "a"] {
        let fields = BTreeMap::from([("pattern".into(), pattern.into())]);
        let result = query.evaluate_with_fields(&labels, "aab", &fields).unwrap();
        if pattern == "[" {
            assert!(
                result.fields.get("__error__").map(String::as_str) == Some("TemplateFormatErr")
            );
        } else {
            let expected = if pattern == "a" { "2" } else { "1" };
            assert!(result.fields.get("total").map(String::as_str) == Some(expected));
        }
    }
}
