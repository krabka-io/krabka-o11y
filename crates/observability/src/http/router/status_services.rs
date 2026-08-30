use super::{Response, StatusCode, text_response};

pub(crate) fn status_services(_name: &'static str) -> Response {
    text_response(
        StatusCode::OK,
        "query-scheduler => Running\n\
         ingester-querier => Running\n\
         query-frontend => Running\n\
         server => Running\n\
         querier => Running\n\
         rule-evaluator => Running\n\
         memberlist-kv => Running\n\
         query-frontend-tripperware => Running\n\
         analytics => Running\n\
         ruler => Running\n\
         cache-generation-loader => Running\n\
         store => Running\n\
         ring => Running\n\
         ingester => Running\n\
         compactor => Running\n\
         distributor => Running\n\
         query-scheduler-ring => Running\n",
    )
}
