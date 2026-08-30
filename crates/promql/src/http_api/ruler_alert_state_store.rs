use super::{AlertStateKey, BTreeMap};

pub(crate) type RulerAlertStateStore = BTreeMap<AlertStateKey, i64>;
