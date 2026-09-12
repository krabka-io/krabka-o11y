use super::{Response, json, success_data_response};

pub(crate) async fn build_info() -> Response {
    success_data_response(json!({
        "version": env!("CARGO_PKG_VERSION"),
        "revision": build_value(option_env!("GIT_SHA").or(option_env!("GITHUB_SHA"))),
        "branch": build_value(option_env!("GIT_BRANCH").or(option_env!("GITHUB_REF_NAME"))),
        "buildUser": build_value(option_env!("BUILD_USER")),
        "buildDate": build_value(option_env!("BUILD_DATE")),
        "goVersion": "not applicable (Rust)",
    }))
}

fn build_value(value: Option<&'static str>) -> &'static str {
    value.filter(|value| !value.is_empty()).unwrap_or("unknown")
}
