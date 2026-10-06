use super::{
    TemplatePart, TemplateRange, TemplateRenderContext, TemplateRuntimeValue,
    template_range::Entries,
};

enum Frame<'a> {
    Parts(&'a [TemplatePart], usize, TemplateRenderContext<'a>),
    Range(&'a TemplateRange, Entries, TemplateRenderContext<'a>),
}

pub(crate) fn render_template_parts<'a>(
    parts: &'a [TemplatePart],
    context: &TemplateRenderContext<'a>,
) -> Vec<u8> {
    let mut rendered = Vec::new();
    let mut frames = vec![Frame::Parts(parts, 0, context.clone())];
    while let Some(frame) = frames.pop() {
        if context.error.borrow().is_some() {
            break;
        }
        match frame {
            Frame::Range(range, mut entries, context) => {
                if context.flow.replace(0) == 1 {
                    continue;
                }
                if let Some((key, value)) = entries.next() {
                    let child = range.iteration(&context, key, value);
                    frames.push(Frame::Range(range, entries, context));
                    frames.push(Frame::Parts(&range.parts, 0, child));
                }
            }
            Frame::Parts(parts, index, mut context) => {
                if context.flow.get() != 0 {
                    continue;
                }
                let Some(part) = parts.get(index) else {
                    continue;
                };
                // Continuations own scopes on the heap, so the Go depth limit never
                // consumes the service thread's call stack.
                let frame_count = frames.len();
                match part {
                    TemplatePart::Literal(literal) => {
                        rendered.extend_from_slice(literal.as_bytes());
                    }
                    TemplatePart::Comment | TemplatePart::Definition { .. } => {}
                    TemplatePart::Break => context.flow.set(1),
                    TemplatePart::Continue => context.flow.set(2),
                    TemplatePart::Expression(expression) => {
                        rendered.extend(expression.evaluate(&context).rendered_bytes());
                    }
                    TemplatePart::Assignment(assignment) => {
                        let value = assignment.expression.evaluate(&context);
                        if assignment.declare {
                            context = context.with_variable(assignment.variable.clone(), value);
                        } else {
                            context.assign_variable(&assignment.variable, value);
                        }
                    }
                    TemplatePart::Invocation { name, argument } => {
                        if context.depth >= 100_000 {
                            *context.error.borrow_mut() =
                                Some("template execution depth exceeded".into());
                        } else if let Some(parts) =
                            context.templates.and_then(|templates| templates.get(name))
                        {
                            let value = argument.as_ref().map_or(
                                TemplateRuntimeValue::Json(serde_json::Value::Null),
                                |argument| argument.evaluate(&context),
                            );
                            let child = context.for_invocation(value);
                            frames.push(Frame::Parts(parts, 0, child));
                        } else {
                            *context.error.borrow_mut() =
                                Some(format!("template {name} is not defined"));
                        }
                    }
                    TemplatePart::Conditional(conditional) => {
                        let mut child = context.clone();
                        let mut selected = &conditional.else_parts;
                        for (condition, parts) in &conditional.branches {
                            let (value, branch) = condition.evaluate(&child);
                            child = branch;
                            if value.is_truthy() {
                                selected = parts;
                                break;
                            }
                        }
                        frames.push(Frame::Parts(selected, 0, child));
                    }
                    TemplatePart::With(with) => {
                        let (value, child) = with.expression.evaluate(&context);
                        if value.is_truthy() {
                            frames.push(Frame::Parts(
                                &with.parts,
                                0,
                                child.with_current_dot(value),
                            ));
                        } else {
                            frames.push(Frame::Parts(&with.else_parts, 0, child));
                        }
                    }
                    TemplatePart::Range(range) => {
                        let value = range.expression.evaluate(&context);
                        let child = range.context(&context, value.clone());
                        let mut entries = range.entries(&child, value).peekable();
                        if entries.peek().is_some() {
                            frames.push(Frame::Range(range, Box::new(entries), child));
                        } else {
                            frames.push(Frame::Parts(&range.else_parts, 0, child));
                        }
                    }
                }
                // Place the caller below any child frame just scheduled.
                if frames.len() > frame_count
                    && let Some(child) = frames.pop()
                {
                    frames.push(Frame::Parts(parts, index + 1, context));
                    frames.push(child);
                } else {
                    frames.push(Frame::Parts(parts, index + 1, context));
                }
            }
        }
    }
    rendered
}
