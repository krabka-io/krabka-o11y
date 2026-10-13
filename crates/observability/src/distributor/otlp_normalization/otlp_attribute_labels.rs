use super::{DistributorError, Labels, normalize_otlp_attribute_name};

pub(super) fn attributes_to_labels<A, K, C, V>(
    attributes: Option<&[A]>,
    key: K,
    children: C,
    label_value: V,
) -> Result<Labels, DistributorError>
where
    K: for<'a> Fn(&'a A) -> &'a str,
    C: for<'a> Fn(&'a A) -> Option<&'a [A]>,
    V: for<'a> Fn(&'a A) -> String,
{
    let mut labels = Labels::new();
    insert_attributes(
        &mut labels,
        "",
        attributes.unwrap_or_default(),
        &key,
        &children,
        &label_value,
    )?;
    Ok(labels)
}

fn insert_attributes<A, K, C, V>(
    labels: &mut Labels,
    prefix: &str,
    attributes: &[A],
    key_of: &K,
    children_of: &C,
    label_value: &V,
) -> Result<(), DistributorError>
where
    K: for<'a> Fn(&'a A) -> &'a str,
    C: for<'a> Fn(&'a A) -> Option<&'a [A]>,
    V: for<'a> Fn(&'a A) -> String,
{
    for attribute in attributes {
        if key_of(attribute).is_empty() {
            return Err(DistributorError::InvalidOtlpAttribute);
        }
        let key = if prefix.is_empty() {
            key_of(attribute).to_string()
        } else {
            format!("{prefix}.{}", key_of(attribute))
        };
        if let Some(children) = children_of(attribute) {
            insert_attributes(labels, &key, children, key_of, children_of, label_value)?;
        } else if labels
            .insert(normalize_otlp_attribute_name(&key), label_value(attribute))
            .is_some()
        {
            return Err(DistributorError::InvalidOtlpAttribute);
        }
    }
    Ok(())
}
