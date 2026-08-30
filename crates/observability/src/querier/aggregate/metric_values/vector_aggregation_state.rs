use super::{MetricValue, Ordering, VectorAggregationOp};

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct VectorAggregationState {
    pub(crate) count: u64,
    pub(crate) sum: MetricValue,
    pub(crate) sum_squares: MetricValue,
    pub(crate) min: Option<MetricValue>,
    pub(crate) max: Option<MetricValue>,
}

impl VectorAggregationState {
    pub(crate) fn record(&mut self, value: MetricValue) {
        self.count += 1;
        self.sum = self.sum.add(value);
        self.sum_squares = self.sum_squares.add(value.multiply(value));
        self.min = Some(self.min.map_or(value, |min| {
            if value.cmp_value(min) == Ordering::Less {
                value
            } else {
                min
            }
        }));
        self.max = Some(self.max.map_or(value, |max| {
            if value.cmp_value(max) == Ordering::Greater {
                value
            } else {
                max
            }
        }));
    }

    pub(crate) fn finish(self, op: &VectorAggregationOp) -> MetricValue {
        match op {
            VectorAggregationOp::Sum => self.sum,
            VectorAggregationOp::Count => MetricValue::integer(self.count),
            VectorAggregationOp::Min => self.min.unwrap_or_else(MetricValue::zero),
            VectorAggregationOp::Max => self.max.unwrap_or_else(MetricValue::zero),
            VectorAggregationOp::Avg => self.sum.divide_by(self.count),
            VectorAggregationOp::Stddev => self.stdvar().sqrt(),
            VectorAggregationOp::Stdvar => self.stdvar(),
            VectorAggregationOp::TopK(_)
            | VectorAggregationOp::BottomK(_)
            | VectorAggregationOp::ApproxTopK(_)
            | VectorAggregationOp::CountValues(_)
            | VectorAggregationOp::Sort
            | VectorAggregationOp::SortDesc => {
                unreachable!("selection aggregations are handled before reduction")
            }
        }
    }

    pub(crate) fn stdvar(self) -> MetricValue {
        if self.count == 0 {
            return MetricValue::zero();
        }

        let mean = self.sum.divide_by(self.count);
        self.sum_squares
            .divide_by(self.count)
            .saturating_sub(mean.multiply(mean))
    }
}
