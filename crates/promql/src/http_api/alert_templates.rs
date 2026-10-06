use std::collections::BTreeMap;

use krabka_blockstore::Labels;

mod expand_alert_template;

pub(crate) use expand_alert_template::expand_alert_template;
#[cfg(test)]
pub(crate) use expand_alert_template::expand_alert_template_with_external;
