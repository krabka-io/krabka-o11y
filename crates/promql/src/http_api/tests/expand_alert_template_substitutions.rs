use super::*;

#[test]
pub(crate) fn expand_alert_template_substitutions() {
    let mut labels = Labels::new();
    labels.insert("job", "api");
    labels.insert("instance", "host-1");

    assert2::assert!(
        expand_alert_template("value is {{ $value }}", 42.5, &labels) == "value is 42.5"
    );
    assert2::assert!(expand_alert_template("job={{ $labels.job }}", 1.0, &labels) == "job=api");
    assert2::assert!(expand_alert_template("job={{ $labels.\"job\" }}", 1.0, &labels) == "job=api");
    // Absent label expands to empty string.
    assert2::assert!(expand_alert_template("x={{ $labels.missing }}", 1.0, &labels) == "x=");
    // Unknown actions pass through verbatim.
    assert2::assert!(
        expand_alert_template("{{ humanize $value }}", 1.0, &labels) == "{{ humanize $value }}"
    );
    // No-whitespace variants still expand.
    assert2::assert!(expand_alert_template("{{$value}} {{$labels.job}}", 7.0, &labels) == "7 api");
}
