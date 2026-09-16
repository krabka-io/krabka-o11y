#!/usr/bin/env python3
"""Detect stable upstream releases and write immutable review proposals."""

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
UPSTREAM = ROOT / "docs/api/upstream_surfaces.json"
CLIENTS = ROOT / "docs/api/client_oracles.json"
SHA = re.compile(r"^[0-9a-f]{40}$")
DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")
PRODUCTS = {
    "alloy": ("grafana/alloy", "/bin/alloy", ["version"], True, ["//crates/integration:alloy_client_docker_test"]),
    "grafana": ("grafana/grafana", "/usr/share/grafana/bin/grafana", ["--version"], False, ["//crates/observability:grafana_e2e_docker_test"]),
    "loki": ("grafana/loki", "/usr/bin/loki", ["-version"], True, ["//crates/observability:loki_differential_docker_test"]),
    "mimir": ("grafana/mimir", "/bin/mimir", ["-version"], True, ["//crates/metrics-service:diff_mimir_docker_test"]),
    "prometheus": ("prometheus/prometheus", "/bin/prometheus", ["--version"], True, ["//crates/metrics-service:diff_prometheus_docker_test"]),
    "pyroscope": ("grafana/pyroscope", "/usr/bin/pyroscope", ["-version"], True, ["//crates/profiles:pyroscope_differential_docker_test"]),
    "tempo": ("grafana/tempo", "/tempo", ["-version"], True, ["//crates/traces:tempo_differential_docker_test"]),
}
SURFACE = re.compile(r"(?:http|grpc|connect|protocol|\.proto|route|Register)", re.IGNORECASE)


def fail(message):
    raise ValueError(message)


def load(path):
    with path.open(encoding="utf-8") as source:
        return json.load(source)


def baselines():
    upstream = load(UPSTREAM)["products"]
    clients = load(CLIENTS)["products"]
    return upstream | {name: value["current"] | {"classifications": value["classifications"]} for name, value in clients.items()}


def validate():
    baseline = baselines()
    if set(baseline) != set(PRODUCTS):
        fail("drift products differ from the public oracle manifests")
    for name, value in baseline.items():
        if not SHA.fullmatch(value.get("revision", "")):
            fail(f"{name} has no immutable source revision")
        if value.get("platform") != "linux/amd64" or not DIGEST.fullmatch(value.get("digest", "")):
            fail(f"{name} has no immutable linux/amd64 image")

    otlp = load(CLIENTS).get("protocols", {}).get("otlp", {})
    for window in ("current", "previous"):
        pin = otlp.get(window, {})
        if not SHA.fullmatch(pin.get("revision", "")) or not re.fullmatch(r"[0-9a-f]{64}", pin.get("sha256", "")):
            fail(f"OTLP {window} has no immutable source identity")


def github(path):
    request = urllib.request.Request(f"https://api.github.com{path}")
    request.add_header("Accept", "application/vnd.github+json")
    if token := os.environ.get("GITHUB_TOKEN"):
        request.add_header("Authorization", f"Bearer {token}")
    with urllib.request.urlopen(request, timeout=60) as response:
        return json.load(response)


def latest_release(repo):
    releases = github(f"/repos/{repo}/releases?per_page=20")
    stable = []
    for release in releases:
        version = re.search(r"\d+\.\d+(?:\.\d+)?", release["tag_name"])
        if version and not release["draft"] and not release["prerelease"]:
            stable.append((tuple(map(int, version.group().split("."))), release))
    if not stable:
        fail(f"{repo} has no stable versioned release")
    return max(stable, key=lambda item: item[0])[1]


def tag_revision(repo, tag):
    obj = github(f"/repos/{repo}/git/ref/tags/{tag}")["object"]
    while obj["type"] == "tag":
        obj = github(f"/repos/{repo}/git/tags/{obj['sha']}")["object"]
    if obj["type"] != "commit" or not SHA.fullmatch(obj["sha"]):
        fail(f"{repo} {tag} does not resolve to an immutable commit")
    return obj["sha"]


def release_image(baseline, tag):
    repository, old_image_tag = baseline["image"].rsplit(":", 1)
    if old_image_tag == baseline["tag"]:
        image_tag = tag
    elif old_image_tag == baseline["tag"].removeprefix("v"):
        image_tag = tag.removeprefix("v")
    else:
        image_tag = tag.removeprefix("mimir-")
    return f"{repository}:{image_tag}"


def verify_reported_identity(image, reported, tag, revision, reports_revision):
    wanted = re.search(r"\d+\.\d+(?:\.\d+)?", tag)
    if not wanted or wanted.group() not in reported:
        fail(f"{image} reports an unexpected version: {reported}")
    if reports_revision and revision[:7] not in reported:
        fail(f"{image} does not report source revision {revision}: {reported}")


def image_identity(image, binary, version_args, tag, revision, reports_revision):
    raw = subprocess.run(
        ["docker", "buildx", "imagetools", "inspect", "--raw", image],
        check=True,
        stdout=subprocess.PIPE,
    ).stdout
    manifest = json.loads(raw)
    manifests = manifest.get("manifests", [])
    matches = [
        item["digest"]
        for item in manifests
        if item.get("platform", {}).get("os") == "linux" and item.get("platform", {}).get("architecture") == "amd64"
    ]
    if manifests and len(matches) != 1:
        fail(f"{image} has no unique linux/amd64 manifest")
    digest = matches[0] if manifests else f"sha256:{hashlib.sha256(raw).hexdigest()}"
    if not DIGEST.fullmatch(digest):
        fail(f"{image} has no unique linux/amd64 digest")
    version = subprocess.run(
        ["docker", "run", "--rm", "--entrypoint", binary, f"{image.rsplit(':', 1)[0]}@{digest}", *version_args],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    ).stdout.strip()
    verify_reported_identity(image, version, tag, revision, reports_revision)
    return digest, version


def changed_surfaces(repo, old, new):
    comparison = github(f"/repos/{repo}/compare/{old}...{new}")
    protobuf = []
    added = []
    removed = []
    for entry in comparison.get("files", []):
        if entry["filename"].endswith(".proto"):
            protobuf.append(entry["filename"])
        for line in entry.get("patch", "").splitlines():
            target = added if line.startswith("+") else removed if line.startswith("-") else None
            if target is not None and SURFACE.search(line[1:]):
                target.append(f"{entry['filename']}: {line[1:].strip()}")
    return sorted(set(added)), sorted(set(removed)), sorted(set(protobuf))


def known_differences(classifications):
    if isinstance(classifications, dict):
        return classifications.get("known_differences", [])
    return [item["reason"] for item in classifications if item["classification"] == "excluded"]


def proposal(name, baseline, release):
    repo, binary, version_args, reports_revision, suites = PRODUCTS[name]
    tag = release["tag_name"]
    revision = tag_revision(repo, tag)
    image = release_image(baseline, tag)
    digest, reported = image_identity(image, binary, version_args, tag, revision, reports_revision)
    added, removed, protobuf = changed_surfaces(repo, baseline["revision"], revision)
    divergences = known_differences(baseline.get("classifications", {}))
    line = re.search(r"\d+\.\d+", tag)
    if not line:
        fail(f"{name} release tag has no major.minor line: {tag}")
    title = f"Upstream drift: {name} {line.group()}"
    body = "\n".join([
        f"## {name} rebaseline proposal",
        "",
        f"- Tag: `{tag}`",
        f"- Source revision: `{revision}`",
        "- Platform: `linux/amd64`",
        f"- Image: `{image}`",
        f"- Image digest: `{digest}`",
        f"- Reported version: `{reported}`",
        f"- Compare: https://github.com/{repo}/compare/{baseline['revision']}...{revision}",
        "",
        "### Added surfaces",
        *(f"- `{item}`" for item in added),
        *([] if added else ["- None detected in retained patches."]),
        "",
        "### Removed surfaces",
        *(f"- `{item}`" for item in removed),
        *([] if removed else ["- None detected in retained patches."]),
        "",
        "### Protobuf changes",
        *(f"- `{item}`" for item in protobuf),
        *([] if protobuf else ["- None."]),
        "",
        "### Known divergences to review",
        *(f"- {item}" for item in divergences),
        *([] if divergences else ["- None recorded."]),
        "",
        "### Required differential suites",
        *(f"- `{suite}`" for suite in suites),
        "",
        "This is a review proposal only. Automation does not promote oracle pins.",
    ])
    return {"title": title, "body": body, "release_line": line.group(), "revision": revision, "digest": digest}


def otlp_proposal(baseline, release):
    repo = "open-telemetry/opentelemetry-proto"
    tag = release["tag_name"]
    revision = tag_revision(repo, tag)
    source = f"https://github.com/{repo}/archive/{revision}.tar.gz"
    with urllib.request.urlopen(source, timeout=60) as response:
        source_sha = hashlib.sha256(response.read()).hexdigest()
    added, removed, protobuf = changed_surfaces(repo, baseline["revision"], revision)
    line = re.search(r"\d+\.\d+", tag)
    if not line:
        fail(f"OTLP release tag has no major.minor line: {tag}")
    title = f"Upstream drift: otlp {line.group()}"
    body = "\n".join([
        "## OTLP rebaseline proposal",
        "",
        f"- Tag and reported source version: `{tag}`",
        f"- Source revision: `{revision}`",
        "- Platform: `source`",
        f"- Source archive: `{source}`",
        f"- Source SHA-256: `{source_sha}`",
        f"- Compare: https://github.com/{repo}/compare/{baseline['revision']}...{revision}",
        "",
        "### Added surfaces",
        *(f"- `{item}`" for item in added),
        *([] if added else ["- None detected in retained patches."]),
        "",
        "### Removed surfaces",
        *(f"- `{item}`" for item in removed),
        *([] if removed else ["- None detected in retained patches."]),
        "",
        "### Protobuf changes",
        *(f"- `{item}`" for item in protobuf),
        *([] if protobuf else ["- None."]),
        "",
        "### Known divergences to review",
        "- None recorded.",
        "",
        "### Required contract suites",
        *(f"- `{suite}`" for suite in load(CLIENTS)["protocols"]["otlp"]["evidence"]),
        "",
        "This is a review proposal only. Automation does not promote protocol pins.",
    ])
    return {"title": title, "body": body, "release_line": line.group(), "revision": revision, "source_sha256": source_sha}


def discover(output):
    output.mkdir(parents=True, exist_ok=True)
    for old in output.glob("*.json"):
        old.unlink()
    for name, baseline in baselines().items():
        release = latest_release(PRODUCTS[name][0])
        if release["tag_name"] == baseline["tag"]:
            continue
        item = proposal(name, baseline, release)
        (output / f"{name}-{item['release_line']}.json").write_text(json.dumps(item, indent=2) + "\n", encoding="utf-8")
    otlp = load(CLIENTS)["protocols"]["otlp"]["current"]
    release = latest_release("open-telemetry/opentelemetry-proto")
    if release["tag_name"] != otlp["tag"]:
        item = otlp_proposal(otlp, release)
        (output / f"otlp-{item['release_line']}.json").write_text(json.dumps(item, indent=2) + "\n", encoding="utf-8")


def self_test():
    validate()
    sample = {"image": "mirror.gcr.io/grafana/mimir:3.2.1", "tag": "mimir-3.2.1"}
    if release_image(sample, "mimir-3.3.0") != "mirror.gcr.io/grafana/mimir:3.3.0":
        fail("Mimir image tag translation failed")
    if known_differences({"known_differences": ["x"]}) != ["x"]:
        fail("client divergences were lost")
    verify_reported_identity("image", "version 1.2.3 revision abcdef0", "v1.2.3", "abcdef0" + "0" * 33, True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--discover", type=Path)
    args = parser.parse_args()
    try:
        if args.self_test:
            self_test()
            print("upstream drift self-test passed")
        elif args.discover:
            validate()
            discover(args.discover)
            print(f"wrote {len(list(args.discover.glob('*.json')))} drift proposals")
        else:
            validate()
            print("upstream drift configuration is valid")
    except (OSError, ValueError, KeyError, StopIteration, subprocess.CalledProcessError, json.JSONDecodeError) as error:
        print(f"upstream-drift.py: {error}", file=sys.stderr)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
