use super::{BTreeMap, AlertStateKey};

pub(crate) type RulerAlertStateStore = BTreeMap<AlertStateKey, i64>;
