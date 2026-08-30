//! Resolved stack frames and symbol resolution boundary.

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    struct Fixed(Vec<Frame>);

    impl SymbolSource for Fixed {
        fn resolve(&self, _partition: u64, _id: u32) -> Vec<Frame> {
            self.0.clone()
        }
    }

    #[test]
    fn symbol_source_is_object_safe_and_returns_frames() {
        let src: Box<dyn SymbolSource> = Box::new(Fixed(vec![Frame {
            function: "main".to_string(),
            file: "main.go".to_string(),
            line: 10,
        }]));
        let frames = src.resolve(0, 1);
        assert!(frames.len() == 1);
        assert!(frames[0].function == "main");
    }
}

// === split-modules: generated submodules ===
mod frame_type;
mod symbol_source;

pub use frame_type::Frame;
pub use symbol_source::SymbolSource;
