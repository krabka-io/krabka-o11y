use super::*;

#[test]
pub(crate) fn recursive_formatting_preserves_loki_specific_rejections() {
    let queries = [
        r#"sort(label_join(vector(1),"joined","/","app"))"#,
        r#"(label_join(vector(1),"joined","/","app"))"#,
        "sort(vector(-1))",
    ];

    for query in queries {
        check!(
            super::prelude::format_logql_query(query).is_err(),
            "query: {query}"
        );
    }
}
