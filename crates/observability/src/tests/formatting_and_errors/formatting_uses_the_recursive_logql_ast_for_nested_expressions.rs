use super::*;

/// Nested metric functions are formatted from `krabka-logql`'s recursive
/// AST when the older HTTP-layer shape-specific formatters cannot represent
/// the inner expression.
#[test]
pub(crate) fn formatting_uses_the_recursive_logql_ast_for_nested_expressions() {
    let query = concat!(
        r#"label_replace(label_replace(rate({app="web"}[5m]),"inner","$1","app","(.*)"),"#,
        r#""outer","$1","inner","(.*)")"#,
    );

    check!(
        super::prelude::format_logql_query(query).expect("the nested expression formats")
            == concat!(
                r#"label_replace(label_replace(rate({app="web"}[5m]), "inner", "$1", "app", "(.*)"), "#,
                r#""outer", "$1", "inner", "(.*)")"#,
            )
    );
}
