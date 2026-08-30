use super::*;

pub fn router<S>(state: Arc<QuerierState<S>>) -> Router
where
    S: ProfileStore + 'static,
{
    let querier = pb::querier::v1::querier_service_connect::QuerierServiceBuilder::<()>::new()
        .profile_types(profile_types_handler::<S>)
        .label_names(label_names_handler::<S>)
        .label_values(label_values_handler::<S>)
        .series(series_handler::<S>)
        .select_merge_stacktraces(select_merge_stacktraces_handler::<S>)
        .select_merge_span_profile(select_merge_span_profile_handler::<S>)
        .select_merge_profile(select_merge_profile_handler::<S>)
        .select_series(select_series_handler::<S>)
        .select_heatmap(select_heatmap_handler::<S>)
        .diff(diff_handler::<S>)
        .get_profile_stats(get_profile_stats_handler::<S>)
        .analyze_query(analyze_query_handler::<S>)
        // `build_connect()` applies the `ConnectLayer` (protocol detection + per-request
        // `ConnectContext`); plain `.build()` omits it, which makes every Connect response
        // fall back to `application/json` regardless of the request's content-type and breaks
        // proto clients like Grafana's built-in Pyroscope datasource (a connect-go client).
        .build_connect();

    // Pyroscope `settings.v1.SettingsService`. The Grafana Profiles Drilldown
    // app calls `Get` during init; a 404 aborts its init chain so it never
    // issues the per-panel `SelectSeries` queries and the landing grid renders
    // empty. Krabka doesn't persist UI settings, so `Get` returns an empty set
    // (the app falls back to its defaults) and `Set` echoes the value back.
    let settings = pb::settings::v1::settings_service_connect::SettingsServiceBuilder::<()>::new()
        .get(get_settings_handler)
        .set(set_settings_handler)
        .build_connect();

    Router::new()
        .route("/pyroscope/render", get(render_handler::<S>))
        .route("/pyroscope/render-diff", get(render_diff_handler::<S>))
        .merge(querier)
        .merge(settings)
        .layer(Extension(state))
}
