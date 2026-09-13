#!/usr/bin/env python3
"""Validate the classified, versioned upstream API surface inventory."""

import argparse
import copy
import json
import pathlib
import re
import sys


ROOT = pathlib.Path(__file__).resolve().parent.parent
MANIFEST = ROOT / "docs" / "api" / "upstream_surfaces.json"
IMAGES = ROOT / "bazel" / "images" / "images.bzl"
MODULE = ROOT / "MODULE.bazel"
CLASSIFICATIONS = {"required", "role-equivalent", "excluded"}
PRODUCTS = {"loki", "mimir", "pyroscope", "tempo"}


def errors(manifest):
    failures = []
    if manifest.get("schema_version") != 1:
        failures.append("schema_version must be 1")
    products = manifest.get("products")
    if not isinstance(products, dict):
        return failures + ["products must be an object"]
    if set(products) != PRODUCTS:
        failures.append(f"products must be exactly {', '.join(sorted(PRODUCTS))}")
    for product in sorted(PRODUCTS):
        data = products.get(product)
        if not isinstance(data, dict):
            failures.append(f"{product}: product entry must be an object")
            continue
        tag = data.get("tag")
        if not isinstance(tag, str) or not tag:
            failures.append(f"{product}: tag must be a non-empty string")
            tag = ""
        image = data.get("image")
        if not isinstance(image, str) or not re.fullmatch(r"[^:@]+(?:/[^:@]+)+:[^:@]+", image):
            failures.append(f"{product}: image must include a registry, repository, and tag")
        elif tag:
            image_tag = image.rsplit(":", 1)[1]
            release = tag.removeprefix("mimir-").removeprefix("v")
            if image_tag != release:
                failures.append(f"{product}: tag and image tag differ")
        if data.get("platform") != "linux/amd64":
            failures.append(f"{product}: platform must be linux/amd64")
        if not re.fullmatch(r"sha256:[0-9a-f]{64}", str(data.get("digest", ""))):
            failures.append(f"{product}: digest must be a sha256 digest")
        if not re.fullmatch(r"[0-9a-f]{40}", str(data.get("revision", ""))):
            failures.append(f"{product}: revision must be a Git commit")
        source = data.get("source")
        if not isinstance(source, str) or tag not in source:
            failures.append(f"{product}: source must name tagged upstream source {tag}")
        surfaces = data.get("surfaces")
        if not isinstance(surfaces, list) or not surfaces:
            failures.append(f"{product}: surfaces must be a non-empty array")
            continue
        if not all(isinstance(surface, str) and surface for surface in surfaces):
            failures.append(f"{product}: every surface must be a non-empty string")
            continue
        if len(surfaces) != len(set(surfaces)):
            failures.append(f"{product}: surface ids must be unique")
        if surfaces != sorted(surfaces):
            failures.append(f"{product}: surfaces must be sorted by id")
        classified = []
        groups = data.get("classifications")
        if not isinstance(groups, list) or not groups:
            failures.append(f"{product}: classifications must be a non-empty array")
            continue
        for index, group in enumerate(groups):
            label = f"{product}: classification {index + 1}"
            if not isinstance(group, dict):
                failures.append(f"{label} must be an object")
                continue
            classification = group.get("classification")
            if classification not in CLASSIFICATIONS:
                failures.append(
                    f"{label} must be " + ", ".join(sorted(CLASSIFICATIONS))
                )
            if classification != "required" and not group.get("reason"):
                failures.append(f"{label} needs a reason for {classification}")
            evidence = group.get("evidence")
            if not isinstance(evidence, list) or not evidence or not all(
                isinstance(item, str)
                and item.startswith("https://github.com/grafana/")
                and tag in item
                for item in evidence
            ):
                failures.append(
                    f"{label} evidence must contain tagged grafana source URLs"
                )
            members = group.get("surfaces")
            if not isinstance(members, list) or not all(
                isinstance(member, str) and member for member in members
            ):
                failures.append(f"{label} surfaces must be an array of strings")
                continue
            classified.extend(members)
        duplicates = sorted(
            {surface for surface in classified if classified.count(surface) > 1}
        )
        if duplicates:
            failures.append(f"{product}: classified more than once: {', '.join(duplicates)}")
        unclassified = sorted(set(surfaces) - set(classified))
        if unclassified:
            failures.append(f"{product}: unclassified surfaces: {', '.join(unclassified)}")
        unknown = sorted(set(classified) - set(surfaces))
        if unknown:
            failures.append(
                f"{product}: classifications name unknown surfaces: {', '.join(unknown)}"
            )
    return failures


def parse_oracles(text):
    try:
        block = text.split("ORACLES = {", 1)[1].split("\n}\n", 1)[0]
    except IndexError:
        return {}
    oracles = {}
    for match in re.finditer(r'"([^"]+)":\s*struct\((.*?)\n\s*\),', block, re.DOTALL):
        fields = dict(re.findall(r'(image|revision)\s*=\s*"([^"]+)",', match.group(2)))
        if fields:
            oracles[match.group(1)] = fields
    return oracles


def parse_pulls(text):
    return {
        name: {"image": f"{registry}/{repository}:{tag}", "digest": digest}
        for name, registry, repository, tag, digest in re.findall(
            r'\("([^"]+)",\s*"([^"]+)",\s*"([^"]+)",\s*"([^"]+)",\s*"(sha256:[0-9a-f]{64})"\)',
            text,
        )
    }


def repository_errors(manifest, images_text, module_text):
    failures = []
    products = manifest.get("products", {})
    oracles = parse_oracles(images_text)
    pulls = parse_pulls(module_text)
    for product in sorted(PRODUCTS):
        baseline = products.get(product)
        if not isinstance(baseline, dict):
            continue
        oracle = oracles.get(product)
        if oracle is None:
            failures.append(f"bazel/images/images.bzl: missing {product} oracle")
        else:
            for field in ("image", "revision"):
                if oracle.get(field) != baseline.get(field):
                    failures.append(
                        f"bazel/images/images.bzl: {product} {field} differs from manifest"
                    )
        pull = pulls.get(product)
        if pull is None:
            failures.append(f"MODULE.bazel: missing {product} image pull")
        else:
            if pull["image"] != baseline.get("image"):
                failures.append(f"MODULE.bazel: {product} image differs from manifest")
            if pull["digest"] != baseline.get("digest"):
                failures.append(f"MODULE.bazel: {product} digest differs from manifest")
    return failures


def load_manifest():
    try:
        return json.loads(MANIFEST.read_text())
    except (OSError, json.JSONDecodeError) as error:
        raise SystemExit(f"{MANIFEST.relative_to(ROOT)}: {error}") from error


def check(manifest, images_text, module_text):
    failures = errors(manifest) + repository_errors(manifest, images_text, module_text)
    if failures:
        for failure in failures:
            print(f"::error::{failure}", file=sys.stderr)
        raise SystemExit(1)
    counts = {
        classification: sum(
            group["classification"] == classification
            for product in manifest["products"].values()
            for group in product["classifications"]
            for _surface in group["surfaces"]
        )
        for classification in sorted(CLASSIFICATIONS)
    }
    total = sum(counts.values())
    summary = ", ".join(f"{key}={value}" for key, value in counts.items())
    print(f"upstream surface manifest is classified ({total} surfaces; {summary})")


def self_test():
    revisions = {
        product: str(index) * 40 for index, product in enumerate(sorted(PRODUCTS), 1)
    }
    valid = {
        "schema_version": 1,
        "products": {
            product: {
                "tag": f"v1.0.{index}",
                "image": f"mirror.gcr.io/grafana/{product}:1.0.{index}",
                "platform": "linux/amd64",
                "digest": f"sha256:{index:064x}",
                "revision": revisions[product],
                "source": f"https://example.invalid/blob/v1.0.{index}/api.md",
                "surfaces": ["http GET /ready"],
                "classifications": [
                    {
                        "classification": "required",
                        "evidence": [
                            f"https://github.com/grafana/{product}/blob/v1.0.{index}/api.md"
                        ],
                        "surfaces": ["http GET /ready"],
                    }
                ],
            }
            for index, product in enumerate(sorted(PRODUCTS), 1)
        },
    }
    assert errors(valid) == []
    mismatched_tag = copy.deepcopy(valid)
    mismatched_tag["products"]["loki"]["tag"] = "v9.9.9"
    assert "tag and image tag differ" in "\n".join(errors(mismatched_tag))
    invalid_product = copy.deepcopy(valid)
    invalid_product["products"]["pyroscope"] = None
    assert "product entry must be an object" in "\n".join(errors(invalid_product))
    unclassified = copy.deepcopy(valid)
    unclassified["products"]["mimir"]["surfaces"].append("http POST /write")
    assert "unclassified surfaces" in "\n".join(errors(unclassified))
    unexplained = copy.deepcopy(valid)
    unexplained["products"]["loki"]["classifications"][0]["classification"] = "excluded"
    assert "needs a reason" in "\n".join(errors(unexplained))
    duplicate = copy.deepcopy(valid)
    duplicate["products"]["tempo"]["classifications"].append(
        {"classification": "required", "surfaces": ["http GET /ready"]}
    )
    assert "classified more than once" in "\n".join(errors(duplicate))
    images_text = "ORACLES = {\n" + "".join(
        f'    "{product}": struct(\n'
        f'        image = "{data["image"]}",\n'
        f'        revision = "{data["revision"]}",\n'
        "    ),\n"
        for product, data in valid["products"].items()
    ) + "}\n"
    module_text = "\n".join(
        f'("{product}", "mirror.gcr.io", "grafana/{product}", '
        f'"1.0.{index}", "{data["digest"]}")'
        for index, (product, data) in enumerate(valid["products"].items(), 1)
    )
    assert repository_errors(valid, images_text, module_text) == []
    drifted_images = images_text.replace("loki:1.0.1", "loki:old", 1)
    assert "loki image differs" in "\n".join(
        repository_errors(valid, drifted_images, module_text)
    )
    drifted_revision = images_text.replace(revisions["mimir"], "f" * 40, 1)
    assert "mimir revision differs" in "\n".join(
        repository_errors(valid, drifted_revision, module_text)
    )
    drifted_module = module_text.replace(valid["products"]["tempo"]["digest"], "sha256:" + "f" * 64)
    assert "tempo digest differs" in "\n".join(
        repository_errors(valid, images_text, drifted_module)
    )
    print("upstream surface self-test passed")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        self_test()
    elif args.check:
        check(load_manifest(), IMAGES.read_text(), MODULE.read_text())
    else:
        parser.error("one of --check or --self-test is required")


if __name__ == "__main__":
    main()
