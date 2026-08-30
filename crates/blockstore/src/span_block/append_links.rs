use super::{
    BlockStoreError, FixedSizeBinaryBuilder, ListBuilder, Result, SpanLink, StructBuilder,
    append_kv,
};

pub(crate) fn append_links(
    links: &mut ListBuilder<StructBuilder>,
    rows: &[SpanLink],
) -> Result<()> {
    let sb = links.values();
    for link in rows {
        sb.field_builder::<FixedSizeBinaryBuilder>(0)
            .expect("linked trace id builder")
            .append_value(link.linked_trace_id)
            .map_err(|e| BlockStoreError::InvalidBlock(e.to_string()))?;
        sb.field_builder::<FixedSizeBinaryBuilder>(1)
            .expect("linked span id builder")
            .append_value(link.linked_span_id)
            .map_err(|e| BlockStoreError::InvalidBlock(e.to_string()))?;
        append_kv(sb, &link.attrs);
        sb.append(true);
    }
    links.append(true);
    Ok(())
}
