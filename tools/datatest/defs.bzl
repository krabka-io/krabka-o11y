"""Corpus staging for `datatest_stable` suites.

`datatest_stable` finds its cases by walking the corpus directory and keeping
the entries whose type is a regular file. Every input Bazel hands a test is a
symlink, in the runfiles tree and again inside the sandbox, so the walk yields
nothing and the suite dies with "no test cases found" over a corpus that is
sitting right there in its own runfiles.

Nothing in the build can make those real files -- a copy under Bazel is a copy
into another tree that is then symlinked in turn -- so the last copy is left to
`//tools/datatest:run_datatest.sh`, which dereferences into the test's scratch
directory just before the suite starts. What this rule contributes is the
layout: one directory holding exactly the corpus, at the workspace paths the
suites are written against, so the wrapper has a single small thing to copy.
"""

def _datatest_corpus_impl(ctx):
    out = ctx.actions.declare_directory(ctx.label.name)

    # The layout under the tree is the workspace's own, because that is what the
    # corpus paths in the suites are written against: one of them chdirs into
    # its own crate directory and reaches sideways into a sibling crate's corpus
    # with `..`, which resolves nowhere else.
    commands = []
    for src in ctx.files.srcs:
        dest = out.path + "/" + src.short_path
        commands.append("mkdir -p '%s'" % dest.rsplit("/", 1)[0])
        commands.append("cp '%s' '%s'" % (src.path, dest))

    ctx.actions.run_shell(
        outputs = [out],
        inputs = ctx.files.srcs,
        command = "set -euo pipefail\n" + "\n".join(commands or ["true"]),
        mnemonic = "DatatestCorpus",
        progress_message = "Staging datatest corpus for %{label}",
    )

    return [DefaultInfo(
        files = depset([out]),
        runfiles = ctx.runfiles(files = [out]),
    )]

datatest_corpus = rule(
    implementation = _datatest_corpus_impl,
    doc = "Stages corpus files as real files under one tree artifact.",
    attrs = {
        "srcs": attr.label_list(
            allow_files = True,
            doc = "Corpus files, staged at their own workspace paths.",
        ),
    },
)
