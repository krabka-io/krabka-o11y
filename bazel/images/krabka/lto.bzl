"""ThinLTO for the binaries in optimized application images."""

_LTO = "@rules_rust//rust/settings:lto"
_MODE = "//command_line_option:compilation_mode"

def _image_lto_impl(settings, _attr):
    mode = settings[_LTO]
    if mode == "unspecified" and str(settings[_MODE]) == "opt":
        mode = "thin"
    return {_LTO: mode}

_image_lto = transition(
    implementation = _image_lto_impl,
    inputs = [_LTO, _MODE],
    outputs = [_LTO],
)

def _binary_impl(ctx):
    return [DefaultInfo(files = depset(ctx.files.binary))]

image_binary = rule(
    implementation = _binary_impl,
    attrs = {
        "binary": attr.label(mandatory = True, cfg = _image_lto),
        "_allowlist_function_transition": attr.label(
            default = "@bazel_tools//tools/allowlists/function_transition_allowlist",
        ),
    },
)
