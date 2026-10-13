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
    let accessors = AttributeAccessors {
        key: &key,
        children: &children,
        label_value: &label_value,
    };
    insert_attributes(&mut labels, "", attributes.unwrap_or_default(), &accessors)?;
    Ok(labels)
}

struct AttributeAccessors<'a, A> {
    key: &'a (dyn for<'b> Fn(&'b A) -> &'b str + 'a),
    children: &'a (dyn for<'b> Fn(&'b A) -> Option<&'b [A]> + 'a),
    label_value: &'a (dyn for<'b> Fn(&'b A) -> String + 'a),
}

fn insert_attributes<A>(
    labels: &mut Labels,
    prefix: &str,
    attributes: &[A],
    accessors: &AttributeAccessors<'_, A>,
) -> Result<(), DistributorError> {
    for attribute in attributes {
        if (accessors.key)(attribute).is_empty() {
            return Err(DistributorError::InvalidOtlpAttribute);
        }
        let key = if prefix.is_empty() {
            (accessors.key)(attribute).to_string()
        } else {
            format!("{prefix}.{}", (accessors.key)(attribute))
        };
        if let Some(children) = (accessors.children)(attribute) {
            insert_attributes(labels, &key, children, accessors)?;
        } else if labels
            .insert(
                normalize_otlp_attribute_name(&key),
                (accessors.label_value)(attribute),
            )
            .is_some()
        {
            return Err(DistributorError::InvalidOtlpAttribute);
        }
    }
    Ok(())
}
