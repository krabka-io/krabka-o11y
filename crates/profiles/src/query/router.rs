use super::{
    Arc, Extension, ProfileStore, QuerierState, Router, analyze_query_handler,
    delete_debuginfo_handler, delete_recording_rule_handler, delete_settings_handler,
    diff_adhoc_handler, diff_handler, feature_flags_handler, get, get_adhoc_handler,
    get_profile_stats_handler, get_recording_rule_handler, get_settings_handler,
    label_names_handler, label_values_handler, list_adhoc_handler, list_debuginfo_handler,
    list_recording_rules_handler, pb, profile_types_handler, render_diff_handler, render_handler,
    select_heatmap_handler, select_merge_profile_handler, select_merge_span_profile_handler,
    select_merge_stacktraces_handler, select_series_handler, series_handler, set_settings_handler,
    should_initiate_upload_handler, upload_adhoc_handler, upload_debuginfo_handler,
    upload_finished_handler, upsert_recording_rule_handler,
};

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
    // issues the per-panel `SelectSeries` queries. The handlers persist each
    // tenant's settings in the configured object store.
    let settings = pb::settings::v1::settings_service_connect::SettingsServiceBuilder::<()>::new()
        .get(get_settings_handler::<S>)
        .set(set_settings_handler::<S>)
        .delete(delete_settings_handler::<S>)
        .build_connect();

    let recording_rules =
        pb::settings::v1::recording_rules_service_connect::RecordingRulesServiceBuilder::<()>::new(
        )
        .get_recording_rule(get_recording_rule_handler::<S>)
        .list_recording_rules(list_recording_rules_handler::<S>)
        .upsert_recording_rule(upsert_recording_rule_handler::<S>)
        .delete_recording_rule(delete_recording_rule_handler::<S>)
        .build_connect();
    let adhoc = pb::adhocprofiles::v1::ad_hoc_profile_service_connect::AdHocProfileServiceBuilder::<()>::new()
        .upload(upload_adhoc_handler::<S>)
        .get(get_adhoc_handler::<S>)
        .list(list_adhoc_handler::<S>)
        .diff(diff_adhoc_handler::<S>)
        .build_connect();
    let capabilities =
        pb::capabilities::v1::feature_flags_service_connect::FeatureFlagsServiceBuilder::<()>::new(
        )
        .get_feature_flags(feature_flags_handler::<S>)
        .build_connect();
    let debuginfo =
        pb::debuginfo::v1alpha1::debuginfo_service_connect::DebuginfoServiceBuilder::<()>::new()
            .should_initiate_upload(should_initiate_upload_handler::<S>)
            .upload_finished(upload_finished_handler::<S>)
            .list_debuginfo(list_debuginfo_handler::<S>)
            .delete_debuginfo(delete_debuginfo_handler::<S>)
            .build_connect();

    Router::new()
        .route("/pyroscope/render", get(render_handler::<S>))
        .route("/pyroscope/render-diff", get(render_diff_handler::<S>))
        .route(
            "/debuginfo.v1alpha1.DebuginfoService/Upload/{gnu_build_id}",
            axum::routing::post(upload_debuginfo_handler::<S>)
                .layer(axum::extract::DefaultBodyLimit::max(1024 * 1024 * 1024)),
        )
        .merge(querier)
        .merge(settings)
        .merge(recording_rules)
        .merge(adhoc)
        .merge(capabilities)
        .merge(debuginfo)
        .layer(Extension(state))
}
