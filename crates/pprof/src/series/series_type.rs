/// One time series returned by `select_series` in the next slice.
#[derive(Clone, Debug, PartialEq)]
pub struct Series {
    pub labels: Vec<(String, String)>,
    pub points: Vec<(i64, f64)>,
}
