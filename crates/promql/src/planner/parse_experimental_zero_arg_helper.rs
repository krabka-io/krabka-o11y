use super::{Expr, Call, Function, ValueType, FunctionArgs};

pub(crate) fn parse_experimental_zero_arg_helper(query: &str) -> Option<Expr> {
    let name = match query.trim() {
        "start()" => "start",
        "end()" => "end",
        _ => return None,
    };

    Some(Expr::Call(Call {
        func: Function::new(name, vec![], 0, ValueType::Scalar, true),
        args: FunctionArgs::empty_args(),
    }))
}
