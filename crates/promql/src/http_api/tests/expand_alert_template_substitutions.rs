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
    assert2::assert!(
        expand_alert_template(
            "{{ if $labels.job }}{{ humanize $value }} {{ printf \"%s\" $labels.job | title }}{{ end }}",
            1500.0,
            &labels,
        ) == "1.5k Api"
    );
    // No-whitespace variants still expand.
    assert2::assert!(expand_alert_template("{{$value}} {{$labels.job}}", 7.0, &labels) == "7 api");

    let mut external_labels = Labels::new();
    external_labels.insert("cluster", "prod-eu");
    assert2::assert!(
        expand_alert_template_with_external(
            "{{ $externalLabels.cluster }} {{ $externalURL }}",
            1.0,
            &labels,
            &external_labels,
            "https://prom.example",
        ) == "prod-eu https://prom.example"
    );
}
