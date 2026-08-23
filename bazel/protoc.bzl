"""Bridges the registered protoc toolchain to a plain file label.

`toolchains_protoc` registers a prebuilt protoc as a
`@rules_proto//proto:toolchain_type`, which the proto rules consume through
toolchain resolution. A Cargo build script cannot: `prost-build` looks for a
filesystem path in `$PROTOC`. This rule resolves the toolchain and re-exports
the compiler as its default output, so `crate_library` can name one
platform-independent label and still get the prebuilt binary matching whichever
platform the build script executes on.
"""

_PROTO_TOOLCHAIN = "@rules_proto//proto:toolchain_type"

def _protoc_binary_impl(ctx):
    protoc = ctx.toolchains[_PROTO_TOOLCHAIN].proto.proto_compiler
    return [DefaultInfo(files = depset([protoc.executable]))]

protoc_binary = rule(
    implementation = _protoc_binary_impl,
    toolchains = [_PROTO_TOOLCHAIN],
    doc = "The protoc executable from the registered proto toolchain.",
)
