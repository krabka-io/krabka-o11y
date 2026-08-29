use super::*;

impl IntoResponse for HttpQueryError {
    fn into_response(self) -> Response {
        let status = match &self {
            Self::BlockStore(_)
            | Self::InvalidPercentEncoding
            | Self::InvalidDirection(_)
            | Self::InvalidLimit(_)
            | Self::LimitNotPositive
            | Self::InvalidStep
            | Self::InvalidQueryParameter { .. }
            | Self::TailDelayForTooLarge
            | Self::InvalidDurationQueryParameter { .. }
            | Self::InvalidInterval
            | Self::InvalidVolumeAggregation
            | Self::InvalidSinceQueryParameter { .. }
            | Self::InvalidTimestampQueryParameter { .. }
            | Self::InvalidTenant
            | Self::MissingQueryParameter(_)
            | Self::MissingTenant
            | Self::QueryRangeTooLarge { .. }
            | Self::LokiQueryRangeTooLarge { .. }
            | Self::QueryResolutionTooHigh
            | Self::QueryBytesTooLarge { .. }
            | Self::QueryLengthTooLarge { .. }
            | Self::QuerySeriesTooLarge { .. }
            | Self::LokiPlainParse(_)
            | Self::CountValuesQuery
            | Self::Plan(_)
            | Self::Query(QueryError::MetricPipelineError { .. })
            | Self::Parse(_) => StatusCode::BAD_REQUEST,
            Self::QueryAuthorization(QueryAuthorizationError::Unauthorized { .. }) => {
                StatusCode::FORBIDDEN
            }
            Self::ApproxTopKDisabled
            | Self::Arrow(_)
            | Self::QueryAuthorization(QueryAuthorizationError::Unavailable { .. })
            | Self::Query(_)
            | Self::DeleteRequests(_)
            | Self::Rules(_)
            | Self::DeleteFilter(_)
            | Self::LokiParquet(_)
            | Self::Parquet(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::LokiParse { query, source } => {
                return loki_parse_error(StatusCode::BAD_REQUEST, query, source);
            }
            Self::LokiFormatParse { query, source } => {
                return loki_format_query_invalid_response(
                    StatusCode::BAD_REQUEST,
                    &loki_parse_error_text(query, source),
                );
            }
            Self::LokiFormatPlainParse(error) => {
                return loki_format_query_invalid_response(StatusCode::BAD_REQUEST, error);
            }
            Self::LokiFormatMissingQuery => {
                return loki_format_query_invalid_response(
                    StatusCode::BAD_REQUEST,
                    "parse error : syntax error: unexpected $end",
                );
            }
        };
        let error_type = match status {
            StatusCode::BAD_REQUEST => "bad_data",
            StatusCode::FORBIDDEN => "forbidden",
            _ => "server_error",
        };
        if matches!(
            self,
            Self::InvalidDirection(_)
                | Self::InvalidLimit(_)
                | Self::LimitNotPositive
                | Self::InvalidStep
                | Self::TailDelayForTooLarge
                | Self::InvalidDurationQueryParameter { .. }
                | Self::InvalidInterval
                | Self::InvalidVolumeAggregation
                | Self::InvalidSinceQueryParameter { .. }
                | Self::InvalidTimestampQueryParameter { .. }
                | Self::LokiQueryRangeTooLarge { .. }
                | Self::QueryResolutionTooHigh
                | Self::LokiPlainParse(_)
                | Self::ApproxTopKDisabled
                | Self::CountValuesQuery
        ) {
            return text_response(status, &self.to_string());
        }
        if matches!(self, Self::MissingQueryParameter("query")) {
            return text_response(status, "parse error : syntax error: unexpected $end");
        }
        loki_error(status, error_type, &self.to_string())
    }
}
