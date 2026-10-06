use super::Labels;

pub(crate) fn labels_key(labels: &Labels) -> String {
    labels.order_key()
}
