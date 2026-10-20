pub(crate) struct EnvironmentOverride<'a> {
    pub(crate) name: &'a str,
    pub(crate) value: Option<&'a str>,
}

pub(crate) fn with_environment<'a, T>(
    overrides: impl IntoIterator<Item = EnvironmentOverride<'a>>,
    run: impl FnOnce() -> T,
) -> T {
    let overrides = overrides.into_iter().collect::<Vec<_>>();
    with_environment_recursive(&overrides, run)
}

fn with_environment_recursive<T>(
    overrides: &[EnvironmentOverride<'_>],
    run: impl FnOnce() -> T,
) -> T {
    if overrides.is_empty() {
        return run();
    }
    let override_value = &overrides[0];
    temp_env::with_var(override_value.name, override_value.value, || {
        with_environment_recursive(&overrides[1..], run)
    })
}
