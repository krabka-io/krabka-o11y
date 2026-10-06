use super::Frame;

/// A selector names a root-first stack prefix; symbolized frames are leaf-first.
#[must_use]
pub fn stack_matches_call_sites(frames: &[Frame], call_sites: &[String]) -> bool {
    call_sites.len() <= frames.len()
        && frames
            .iter()
            .rev()
            .zip(call_sites)
            .all(|(frame, site)| frame.function == *site)
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    #[test]
    fn stack_prefix_preserves_order_and_repeated_frames() {
        let frames = [
            Frame {
                function: "leaf".into(),
                file: String::new(),
                line: 0,
            },
            Frame {
                function: "root".into(),
                file: String::new(),
                line: 0,
            },
        ];
        check!(stack_matches_call_sites(&frames, &["root".into()]));
        check!(stack_matches_call_sites(
            &frames,
            &["root".into(), "leaf".into()]
        ));
        check!(!stack_matches_call_sites(&frames, &["leaf".into()]));
        check!(!stack_matches_call_sites(
            &frames,
            &["leaf".into(), "root".into()]
        ));
        check!(!stack_matches_call_sites(
            &frames,
            &["root".into(), "root".into()]
        ));
    }
}
