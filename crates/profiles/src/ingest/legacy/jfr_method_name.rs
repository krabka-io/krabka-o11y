use std::sync::LazyLock;

use regex::Regex;

static GENERATED_METHOD_ACCESSOR: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^(jdk/internal/reflect/GeneratedMethodAccessor)\d+$").expect("valid regex")
});
static LAMBDA_CLASS: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^(.+\$\$Lambda)(?:\$?\d*[./](?:0x)?[\da-f]+|\d+)$").expect("valid regex")
});
static CGLIB_CLASS: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^(.+\$\$EnhancerBySpringCGLIB\$\$).*$").expect("valid regex"));
static ZSTD_JNI_LIBRARY: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^(?:\.?/tmp/)?libzstd-jni-\d+\.\d+\.\d+-\d+\.so(?: \(deleted\))?$")
        .expect("valid regex")
});
static AMAZON_CORRETTO_LIBRARY: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"^(?:\.?/tmp/)?(?:lib)?amazonCorrettoCryptoProvider(?:NativeLibraries\.)?[0-9a-f]{16}(?:/libcrypto|/libamazonCorrettoCryptoProvider)?\.so(?: \(deleted\))?$",
    )
    .expect("valid regex")
});
static ASYNC_PROFILER_LIBRARY: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"^(?:\.?/tmp/)?libasyncProfiler-(?:linux-arm64|linux-musl-x64|linux-x64|macos)-17b9a1d8156277a98ccc871afa9a8f69215f92\.so(?: \(deleted\))?$",
    )
    .expect("valid regex")
});

pub(crate) fn jfr_method_name(
    class: Option<&jfrs::reader::types::builtin::Class<'_>>,
    method_name: &str,
) -> String {
    class
        .and_then(|class| class.name.as_ref())
        .and_then(|name| name.string)
        .map_or_else(
            || normalize_symbol(method_name),
            |class_name| {
                format!(
                    "{}.{}",
                    normalize_symbol(class_name),
                    normalize_symbol(method_name)
                )
            },
        )
}

fn normalize_symbol(symbol: &str) -> String {
    let symbol = GENERATED_METHOD_ACCESSOR.replace(symbol, "${1}_");
    let symbol = LAMBDA_CLASS.replace(&symbol, "${1}_");
    let symbol = CGLIB_CLASS.replace(&symbol, "${1}_");
    if ZSTD_JNI_LIBRARY.is_match(&symbol) {
        return "libzstd-jni-_.so".to_string();
    }
    if AMAZON_CORRETTO_LIBRARY.is_match(&symbol) {
        return "libamazonCorrettoCryptoProvider_.so".to_string();
    }
    if ASYNC_PROFILER_LIBRARY.is_match(&symbol) {
        return "libasyncProfiler-_.so".to_string();
    }
    symbol.into_owned()
}

#[cfg(test)]
mod tests {
    use super::normalize_symbol;

    #[test]
    fn normalizes_jvm_generated_classes_like_pyroscope() {
        for (input, expected) in [
            (
                "org/example/Enclosing$$Lambda$4/1283928880",
                "org/example/Enclosing$$Lambda_",
            ),
            (
                "jdk/internal/reflect/GeneratedMethodAccessor31",
                "jdk/internal/reflect/GeneratedMethodAccessor_",
            ),
            (
                "foo/Bar$$EnhancerBySpringCGLIB$$1234567890",
                "foo/Bar$$EnhancerBySpringCGLIB$$_",
            ),
            (
                "libzstd-jni-1.5.1-16931311898282279136.so",
                "libzstd-jni-_.so",
            ),
            (
                "/tmp/libamazonCorrettoCryptoProvider109b39cf33c563eb.so",
                "libamazonCorrettoCryptoProvider_.so",
            ),
            (
                "libasyncProfiler-linux-arm64-17b9a1d8156277a98ccc871afa9a8f69215f92.so",
                "libasyncProfiler-_.so",
            ),
        ] {
            assert_eq!(normalize_symbol(input), expected);
        }
    }
}
