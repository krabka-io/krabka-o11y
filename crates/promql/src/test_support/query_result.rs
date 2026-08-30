use super::*;

impl QueryResultExt for QueryResult {
    fn single(&self) -> &InstantSample {
        let QueryResult::InstantVector(samples) = self else {
            panic!("expected instant vector");
        };
        assert2::assert!(samples.len() == 1);
        &samples[0]
    }

    fn as_scalar(&self) -> f64 {
        let QueryResult::Scalar { value, .. } = self else {
            panic!("expected scalar");
        };
        *value
    }

    fn values_f64(&self) -> Vec<f64> {
        let QueryResult::InstantVector(samples) = self else {
            panic!("expected instant vector");
        };
        samples.iter().map(InstantSampleExt::value_f64).collect()
    }

    fn is_empty(&self) -> bool {
        matches!(self, QueryResult::InstantVector(samples) if samples.is_empty())
    }

    fn iter(&self) -> std::slice::Iter<'_, InstantSample> {
        let QueryResult::InstantVector(samples) = self else {
            panic!("expected instant vector");
        };
        samples.iter()
    }

    fn len(&self) -> usize {
        let QueryResult::InstantVector(samples) = self else {
            panic!("expected instant vector");
        };
        samples.len()
    }
}
