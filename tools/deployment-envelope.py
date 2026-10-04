#!/usr/bin/env python3
"""Measure the public ingest, query, and WAL paths of the Compose deployment.

The storage soak measures compaction, retention, and cold block reads separately.
This harness adds HTTP admission, broker lag, and restart catch-up evidence.
It keeps the configured tenant caps out of the saturation search.
"""

import argparse
import concurrent.futures
import hashlib
import importlib.util
import itertools
import json
import math
import os
import pathlib
import re
import subprocess
import struct
import statistics
import threading
import time
import urllib.error
import urllib.parse
import urllib.request


SIGNALS = ("metrics", "logs", "traces", "profiles")
INGEST = {"metrics": 4041, "logs": 3100, "traces": 4318, "profiles": 4040}
QUERY = {"metrics": 9090, "logs": 3101, "traces": 3201, "profiles": 4042}
ROLES = ("distributor", "block-builder", "querier")
SEED = 267
POINTS = 10
SERIES = 100
WRITERS = (1, 2, 4, 8, 16, 32, 64)
CARDINALITIES = (100, 1_000, 5_000, 20_000)
P99_SECONDS = 2
CATCHUP_SECONDS = 10

# Reuse the repository's protobuf encoder. Importing it does not generate
# corpus files; that work is behind its CLI entry point.
_wire_spec = importlib.util.spec_from_file_location("envelope_wire", pathlib.Path(__file__).with_name("fuzz-corpus.py"))
wire = importlib.util.module_from_spec(_wire_spec)
_wire_spec.loader.exec_module(wire)


def command(*args):
    return subprocess.check_output(args, text=True).strip()


def http(port, path, tenant="soak", body=None, content_type="application/json"):
    headers = {"X-Scope-OrgID": tenant, "Content-Type": content_type}
    if isinstance(body, dict):
        body = json.dumps(body, separators=(",", ":")).encode()
    if isinstance(body, str):
        body = body.encode()
    request = urllib.request.Request(f"http://127.0.0.1:{port}{path}", body, headers)
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return response.status, response.read()
    except urllib.error.HTTPError as error:
        return error.code, error.read()
    except (OSError, TimeoutError) as error:
        return 0, str(error).encode()


def write_request(signal, sequence, cardinality, tenant="soak"):
    now = time.time_ns()
    # Trace searches assemble spans, rather than evaluating a scalar. Keep
    # their starting batch below the query saturation point, then ramp load.
    batch_series = 10 if signal == "traces" else SERIES
    series = [(sequence * batch_series + i) % cardinality for i in range(batch_series)]
    if signal == "metrics":
        lines = [
            f"envelope_samples,series={s} value={(SEED + s + p) % 97} {now // 1_000_000 + p}"
            for s in series for p in range(POINTS)
        ]
        return "/api/v1/push/influx/write?precision=ms", "\n".join(lines), "text/plain", len(lines)
    if signal == "logs":
        streams = [{"stream": {"job": "envelope", "series": str(s)}, "values": [
            [str(now + p), f"envelope-{SEED}-{sequence}-{p}"] for p in range(POINTS)
        ]} for s in series]
        return "/loki/api/v1/push", {"streams": streams}, "application/json", SERIES * POINTS
    if signal == "traces":
        groups = []
        for s in series:
            trace = hashlib.sha256(f"{SEED}-{tenant}-{sequence}-{s}".encode()).digest()[:16]
            attributes = b"".join(wire.pb_bytes_field(1,
                wire.pb_string_field(1, key) + wire.pb_bytes_field(2, wire.pb_string_field(1, value)))
                for key, value in (("service.name", "envelope"), ("series", str(s))))
            spans = b"".join(wire.pb_bytes_field(2,
                wire.pb_bytes_field(1, trace) + wire.pb_bytes_field(2, (p + 1).to_bytes(8, "big"))
                + wire.pb_string_field(5, "envelope")
                + b"\x39" + struct.pack("<Q", now + p)
                + b"\x41" + struct.pack("<Q", now + p + 1_000_000)) for p in range(POINTS))
            groups.append(wire.pb_bytes_field(1, attributes) + wire.pb_bytes_field(2, spans))
        return "/v1/traces", b"".join(wire.pb_bytes_field(1, g) for g in groups), "application/x-protobuf", batch_series * POINTS
    # The legacy API carries one labelled profile per request.
    name = f'envelope{{service_name="envelope",series="{sequence % cardinality}"}}'
    query = urllib.parse.urlencode({"name": name, "format": "groups", "units": "samples",
                                    "until": now // 1_000_000})
    body = "\n".join(f"envelope;frame_{p} 1" for p in range(POINTS))
    return f"/ingest?{query}", body, "text/plain", POINTS


def query_request(signal, window_seconds=30):
    now = time.time()
    if signal == "metrics":
        query = urllib.parse.urlencode({"query": "sum(envelope_samples)", "time": now})
        return f"/api/v1/query?{query}", None
    if signal == "logs":
        query = urllib.parse.urlencode({"query": '{job="envelope"}', "limit": 1000,
                                      "start": int((now - window_seconds) * 1e9), "end": int(now * 1e9)})
        return f"/loki/api/v1/query_range?{query}", None
    if signal == "traces":
        query = urllib.parse.urlencode({"q": '{resource.service.name="envelope"}',
                                      "start": int(now - window_seconds), "end": int(now + 1), "limit": 1000})
        return f"/api/search?{query}", None
    return "/querier.v1.QuerierService/SelectMergeStacktraces", {
        "profileTypeID": "process_cpu:cpu:nanoseconds:cpu:nanoseconds",
        "labelSelector": '{service_name="envelope"}', "start": int((now - window_seconds) * 1000),
        "end": int((now + 1) * 1000), "maxNodes": 1024,
    }


def has_data(signal, body):
    try:
        data = json.loads(body)
        if signal in ("metrics", "logs"):
            return data.get("status") == "success" and bool(data.get("data", {}).get("result"))
        if signal == "traces":
            return bool(data.get("traces"))
        flamegraph = data.get("flamegraph", {})
        return bool(flamegraph.get("levels"))
    except (ValueError, AttributeError, TypeError):
        return False


def quantiles(values):
    values = sorted(values)
    return {"count": len(values), **{
        f"p{q}": values[max(0, math.ceil(len(values) * q / 100) - 1)] if values else None
        for q in (50, 95, 99)
    }}


def summarize(records, duration):
    attempts = len(records)
    errors = sum(r["status"] == 0 or r["status"] >= 300 for r in records)
    accepted = sum(r.get("rows", 0) for r in records if 200 <= r["status"] < 300)
    return {"attempts": attempts, "errors": errors, "error_rate": errors / attempts if attempts else None,
            "accepted_rows": accepted, "accepted_rows_per_sec": accepted / duration,
            "latency_seconds": quantiles([r["seconds"] for r in records]),
            "empty_queries": sum(r.get("has_data") is False for r in records),
            "status_counts": {str(s): sum(r["status"] == s for r in records)
                              for s in sorted({r["status"] for r in records})}}


def prometheus(text):
    samples = {}
    for line in text.splitlines():
        if line and not line.startswith("#"):
            fields = line.split()
            if len(fields) >= 2:
                try:
                    samples[fields[0]] = float(fields[1])
                except ValueError:
                    continue
    return samples


class Deployment:
    def __init__(self, evidence, image):
        self.evidence = evidence.resolve()
        self.evidence.mkdir(parents=True, exist_ok=True)
        self.project = f"envelope-{os.environ.get('GITHUB_RUN_ID', os.getpid())}"
        base = json.loads(command("docker", "compose", "-f", "deploy/compose/docker-compose.yaml",
                                  "config", "--format", "json"))
        base.pop("name", None)
        services = base["services"]
        services.pop("alloy", None)
        roles = self.evidence / "roles"
        roles.mkdir(exist_ok=True)
        for file in pathlib.Path("deploy/roles").glob("*.yaml"):
            (roles / file.name).write_text(file.read_text())
        limits = {
            "metrics": 'defaults:\n  ingestion_rate: "0/s"\n  out_of_order_time_window: "30s"\noverrides:\n  noisy:\n    ingestion_rate: "2000/s"\n',
            "logs": "defaults:\n  max_query_series: 0\noverrides: {}\n",
            "traces": "overrides:\n" + "".join(f"  {tenant}:\n    ingestion_rate_spans_per_sec: 0\n"
                for tenant in ("soak", "quiet", *(f"burst-{n}" for n in WRITERS),
                               *(f"cardinality-{n}" for n in CARDINALITIES)))
                + "  noisy:\n    ingestion_rate_spans_per_sec: 2000\n",
            "profiles": "defaults:\n  ingestion_rate_profiles_per_sec: 0\noverrides:\n  noisy:\n    ingestion_rate_profiles_per_sec: 20\n",
        }
        for signal, content in limits.items():
            (roles / f"{signal}-limits.yaml").write_text(content)
        # The deployment keeps all blocks for the HTTP phases. Retention is
        # qualified by the storage soak, where it cannot erase HTTP fixtures.
        flags = {"metrics": "runtime-overrides", "logs": "logs-limits-overrides-config",
                 "traces": "traces-limits-overrides-config", "profiles": "profiles-limits-overrides-config"}
        for signal in SIGNALS:
            if signal != "logs":
                file = roles / f"{signal}-block-builder.yaml"
                content = file.read_text()
                if "block-builder-flush-max-age:" in content:
                    content = re.sub(r"block-builder-flush-max-age: .*", "block-builder-flush-max-age: 2s", content)
                else:
                    content += "\nblock-builder-flush-max-age: 2s\n"
                file.write_text(content)
            file = roles / f"{signal}-distributor.yaml"
            file.write_text(file.read_text() + f"\n{flags[signal]}: /etc/krabka/{signal}-limits.yaml\n")
            if signal == "logs":
                file = roles / "logs-querier.yaml"
                file.write_text(file.read_text() + "\nlogs-limits-overrides-config: /etc/krabka/logs-limits.yaml\n")
            if signal != "logs":
                services[f"{signal}-compactor"] = {
                    "image": image, "command": [f"krabka-{signal}",
                        f"--config.file=/etc/krabka/{signal}-compactor.yaml"],
                    "environment": dict(services[f"{signal}-block-builder"]["environment"]),
                    "volumes": [{"type": "bind", "source": str(roles), "target": "/etc/krabka", "read_only": True}],
                    "depends_on": services[f"{signal}-block-builder"]["depends_on"],
                }
                file = roles / f"{signal}-compactor.yaml"
                content = file.read_text().replace("5m", "2s")
                file.write_text(content)
        self.admin_ports = {}
        for number, (name, service) in enumerate(sorted(services.items())):
            if "krabka-o11y" in service["image"]:
                service["image"] = image
            service["cpus"] = 2.0 if name in ("broker", "minio") else 1.0
            service["mem_limit"] = "2g" if name in ("broker", "minio") else "1g"
            service["ulimits"] = {"nofile": {"soft": 65536, "hard": 65536}}
            service.pop("restart", None)
            for volume in service.get("volumes", []):
                if volume.get("target") == "/etc/krabka":
                    volume["source"] = str(roles)
            if name == "broker" or name.startswith(tuple(f"{s}-" for s in SIGNALS)):
                self.admin_ports[name] = 15000 + number
                service.setdefault("ports", []).append({"target": 9404, "published": str(15000 + number),
                                                         "host_ip": "127.0.0.1", "protocol": "tcp"})
            # Bind public test ports to the runner's loopback interface.
            for port in service.get("ports", []):
                port["host_ip"] = "127.0.0.1"
        self.file = self.evidence / "compose.json"
        for kind in ("networks", "volumes"):
            for config in base.get(kind, {}).values():
                config.pop("name", None)
        self.file.write_text(json.dumps(base, indent=2))
        self.compose = ["docker", "compose", "-p", self.project, "-f", str(self.file)]
        self.ids = {}
        self.pids = {}
        self.sequence = itertools.count()

    def run(self, *args):
        return command(*self.compose, *args)

    def start(self):
        self.run("up", "-d", "--wait", "--wait-timeout", "300")
        for signal in SIGNALS:
            until = time.monotonic() + 180
            while time.monotonic() < until:
                if http(QUERY[signal], "/ready")[0] == 200:
                    break
                time.sleep(1)
            else:
                raise RuntimeError(f"{signal} querier did not become ready")
        for name in self.admin_ports:
            self.ids[name] = self.run("ps", "-q", name)
        self.ids["minio"] = self.run("ps", "-q", "minio")
        self.refresh_pids()
        images = {name: json.loads(command("docker", "inspect", identity))[0]
                  for name, identity in self.ids.items()}
        (self.evidence / "containers.json").write_text(json.dumps(images, indent=2))

    def refresh_pids(self):
        for name, identity in self.ids.items():
            self.pids[name] = int(command("docker", "inspect", "--format", "{{.State.Pid}}", identity))

    def sample(self, signal):
        samples = {"time_unix": time.time(), "rss_kib": {}, "metrics": {}, "recovery": {}, "scrape_errors": []}
        names = [name for name in self.admin_ports if name == "broker" or name.startswith(signal + "-")]
        for name in names:
            status, body = http(self.admin_ports[name], "/metrics")
            if status != 200:
                samples["scrape_errors"].append(name)
                continue
            samples["metrics"][name] = prometheus(body.decode())
            if name != "broker":
                status, body = http(self.admin_ports[name], "/status/recovery")
                if status == 200:
                    samples["recovery"][name] = json.loads(body)
            try:
                text = pathlib.Path(f"/proc/{self.pids[name]}/status").read_text()
                samples["rss_kib"][name] = int(re.search(r"^VmRSS:\s+(\d+)", text, re.M)[1])
            except (OSError, TypeError):
                samples["scrape_errors"].append(f"{name}/rss")
        try:
            text = pathlib.Path(f"/proc/{self.pids['minio']}/status").read_text()
            samples["rss_kib"]["minio"] = int(re.search(r"^VmRSS:\s+(\d+)", text, re.M)[1])
        except (OSError, TypeError):
            samples["scrape_errors"].append("minio/rss")
        return samples

    def drain(self, signal, timeout=180):
        started = time.monotonic()
        status = None
        while time.monotonic() - started < timeout:
            code, body = http(self.admin_ports[f"{signal}-block-builder"], "/status/recovery")
            if code == 200:
                status = json.loads(body)
                if durably_caught_up(status):
                    return {"recovered": True, "seconds": time.monotonic() - started, "status": status}
            time.sleep(0.25)
        return {"recovered": False, "seconds": time.monotonic() - started, "status": status}

    def close(self):
        (self.evidence / "containers.log").write_text(self.run("logs", "--no-color"))
        self.run("down", "--volumes", "--remove-orphans", "--timeout", "5")


def telemetry(samples, signal):
    topic = "__krabka_observability_logs_wal" if signal == "logs" else f"__krabka_{signal}_wal"
    lag = [v for sample in samples for key, v in sample["metrics"].get("broker", {}).items()
           if key.startswith("krabka_broker_consumer_group_lag_records{") and f'topic="{topic}"' in key]
    first, last = samples[0], samples[-1]
    requests = bytes_ = 0
    for name, metrics in last["metrics"].items():
        if name == "broker":
            continue
        for key, value in metrics.items():
            delta = max(0, value - first["metrics"].get(name, {}).get(key, 0))
            if "_objstore_operations_total" in key:
                requests += delta
            if "_objstore_operation_transferred_bytes_total" in key:
                bytes_ += delta
    return {"wal_lag_records_max": max(lag) if lag else None,
            "wal_lag_records_last": [v for key, v in last["metrics"].get("broker", {}).items()
                                     if key.startswith("krabka_broker_consumer_group_lag_records{")
                                     and f'topic="{topic}"' in key],
            "rss_kib_peak_by_role": {name: max(s["rss_kib"].get(name, 0) for s in samples)
                                     for name in last["rss_kib"]},
            "object_requests": requests, "object_bytes": bytes_,
            "scrape_errors": sum(len(s["scrape_errors"]) for s in samples)}


def durably_caught_up(status):
    consumers = status.get("wal_consumers", [])
    partitions = [p for c in consumers for p in c["partitions"] if p["assigned"]]
    return bool(consumers and partitions) and status["ready"] and all(c["caught_up"] for c in consumers) and all(
        p["consumed_offset"] is not None and p["committed_offset"] is not None
        and p["committed_offset"] > p["consumed_offset"] for p in partitions)


def measure(deployment, signal, seconds, warmup, writers, cardinality=100, noisy=False, cold=False, interval=0.25, tenant="soak"):
    records = []
    stop = threading.Event()
    started = time.monotonic()
    begin = started + warmup
    until = begin + seconds

    def worker(kind, tenant, pace):
        while time.monotonic() < until:
            seq = next(deployment.sequence) if kind == "write" else None
            if kind == "write":
                path, body, content, rows = write_request(signal, seq, cardinality, tenant)
                port = INGEST[signal]
            else:
                path, body = query_request(signal, 1800 if cold else 30)
                content, rows, port = "application/json", 0, QUERY[signal]
            before = time.monotonic()
            status, response = http(port, path, tenant, body, content)
            elapsed = time.monotonic() - before
            if before >= begin:
                records.append({"kind": kind, "tenant": tenant, "sequence": seq,
                                "time_unix": time.time(), "seconds": elapsed,
                                "status": status, "rows": rows,
                                "has_data": has_data(signal, response) if kind == "query" else None,
                                "error": response[:200].decode(errors="replace") if status >= 300 or status == 0 else None})
            if pace:
                stop.wait(max(0, pace - elapsed))

    tenants = [("quiet" if noisy else tenant, writers, interval)]
    if noisy:
        tenants.append(("noisy", 4, 0))
    with concurrent.futures.ThreadPoolExecutor(max_workers=writers + 6) as pool:
        tasks = [pool.submit(worker, "query", "quiet" if noisy else tenant, 0.25)]
        for tenant, count, pace in tenants:
            tasks.extend(pool.submit(worker, "write", tenant, pace) for _ in range(count))
        samples = []
        while time.monotonic() < until:
            if time.monotonic() >= begin:
                samples.append(deployment.sample(signal))
            stop.wait(1)
        for task in tasks:
            task.result()
    duration = time.monotonic() - begin
    samples.append(deployment.sample(signal))
    main = "quiet" if noisy else tenant
    writes = summarize([r for r in records if r["kind"] == "write" and r["tenant"] == main], duration)
    queries = summarize([r for r in records if r["kind"] == "query"], duration)
    result = {"signal": signal, "writers": writers, "cardinality": cardinality,
              "warmup_seconds": warmup, "duration_seconds": duration, "write_interval_seconds": interval,
              "ingest": writes, "query": queries, "telemetry": telemetry(samples, signal),
              "tenant": main,
              "tenants": {t: summarize([r for r in records if r["tenant"] == t and r["kind"] == "write"], duration)
                          for t, _, _ in tenants}}
    result["objectives_met"] = (
        (writers == 0 or writes["attempts"] > 0 and writes["error_rate"] == 0
         and writes["latency_seconds"]["p99"] <= P99_SECONDS)
        and queries["attempts"] > 0 and queries["error_rate"] == 0
        and queries["empty_queries"] == 0 and queries["latency_seconds"]["p99"] <= P99_SECONDS
        and result["telemetry"]["scrape_errors"] == 0
    )
    result["catchup"] = deployment.drain(signal)
    result["ingest"]["durable_rows_per_sec"] = writes["accepted_rows"] / (duration + result["catchup"]["seconds"])
    result["objectives_met"] &= result["catchup"]["recovered"] and result["catchup"]["seconds"] <= CATCHUP_SECONDS
    return result, records, samples


def run(args):
    deployment = Deployment(args.output, args.image)
    report = {"schema_version": 1, "harness_commit": command("git", "rev-parse", "HEAD"),
              "image": args.image, "image_digest": args.image_digest or args.image.split("@")[-1],
              "image_commit": args.image_commit,
              "seed": SEED, "phase_seconds": args.seconds,
              "shape": {"role_cpu": 1, "role_memory_gib": 1, "broker_cpu": 2, "broker_memory_gib": 2,
                        "object_store_cpu": 2, "object_store_memory_gib": 2, "replicas": 1,
                        "wal_partitions": 1, "replication": 1, "wal_retention_seconds": 900,
                        "block_retention": "unlimited", "flush_max_age_seconds": 2, "nofile": 65536,
                        "broker_image": json.loads(deployment.file.read_text())["services"]["broker"]["image"],
                        "object_store_image": json.loads(deployment.file.read_text())["services"]["minio"]["image"]},
              "run_id": f"{os.environ.get('GITHUB_RUN_ID', 'local')}-{os.environ.get('GITHUB_RUN_ATTEMPT', '1')}-{args.output.name}",
              "objectives": {"ingest_p99_seconds": P99_SECONDS, "query_p99_seconds": P99_SECONDS,
                             "error_rate": 0, "durable_catchup_seconds": CATCHUP_SECONDS},
              "dataset": {"points_per_series": POINTS, "series_per_request":
                          {"metrics": SERIES, "logs": SERIES, "traces": 10, "profiles": 1}},
              "host": {"cpu_count": os.cpu_count(), "kernel": command("uname", "-r"),
                       "cpu_model": next(r["data"] for r in json.loads(command("lscpu", "-J"))["lscpu"]
                                         if r["field"] == "Model name:"),
                       "memory_kib": int(re.search(r"^MemTotal:\s+(\d+)", pathlib.Path("/proc/meminfo").read_text(), re.M)[1]),
                       "runner": os.environ.get("KRABKA_SOAK_RUNNER")},
              "burst_levels": WRITERS, "cardinality_levels": CARDINALITIES, "entries": []}
    sequence = 0
    try:
        deployment.start()
        for signal in SIGNALS:
            # Seed the hot tier before the first query can measure an empty result.
            path, body, content, _ = write_request(signal, 0, 100)
            status, response = http(INGEST[signal], path, body=body, content_type=content)
            if not 200 <= status < 300:
                raise RuntimeError(f"{signal} did not accept the initial corpus: {status} {response[:200]!r}")
            deadline = time.monotonic() + 120
            while time.monotonic() < deadline:
                path, body = query_request(signal)
                status, response = http(QUERY[signal], path, body=body)
                if status == 200 and has_data(signal, response):
                    break
                time.sleep(1)
            else:
                raise RuntimeError(f"{signal} query did not see the initial corpus")
            plans = [("steady", 2, 100, False, False)]
            plans += [("burst", level, 100, False, False) for level in WRITERS]
            plans += [("high_cardinality", 2, level, False, False) for level in CARDINALITIES]
            plans += [("cold_window", 0, 100, False, True), ("noisy_tenant", 2, 100, True, False)]
            failed = set()
            last_cardinality_tenant = "soak"
            last_cardinality = 100
            for phase, writers, cardinality, noisy, cold in plans:
                if phase in failed:
                    continue
                print(f"{signal} {phase} writers={writers} cardinality={cardinality}", flush=True)
                tenant = f"burst-{writers}" if phase == "burst" else (
                    f"cardinality-{cardinality}" if phase == "high_cardinality" else (
                        last_cardinality_tenant if cold else "quiet" if noisy else "soak"))
                count = 1
                if phase == "high_cardinality":
                    per_request = report["dataset"]["series_per_request"][signal]
                    count = math.ceil(cardinality / per_request)
                if not cold:
                    for _ in range(count):
                        path, body, content, _ = write_request(signal, next(deployment.sequence), cardinality, tenant)
                        status, response = http(INGEST[signal], path, tenant, body, content)
                        if not 200 <= status < 300:
                            raise RuntimeError(f"{signal} {phase} seed failed: {status} {response[:200]!r}")
                    seeded = deployment.drain(signal)
                    if not seeded["recovered"]:
                        raise RuntimeError(f"{signal} seed did not become durable: {seeded}")
                result, records, samples = measure(deployment, signal, args.seconds, args.seconds / 4,
                                                    writers, cardinality, noisy, cold or phase == "high_cardinality",
                                                    1.0 if phase == "burst" else 0.25, tenant)
                result["phase"] = phase
                result["seed_batches"] = count if not cold else 0
                result["containers_after_phase"] = {name: json.loads(command("docker", "inspect", "--format", "{{json .State}}", identity))
                    for name, identity in deployment.ids.items() if name.startswith(signal + "-")}
                report["entries"].append(result)
                name = f"{sequence:02d}-{signal}-{phase}"
                sequence += 1
                (args.output / f"{name}.operations.jsonl").write_text("".join(json.dumps(r) + "\n" for r in records))
                (args.output / f"{name}.telemetry.jsonl").write_text("".join(json.dumps(s) + "\n" for s in samples))
                (args.output / "deployment-report.json").write_text(json.dumps(report, indent=2))
                if phase in ("burst", "high_cardinality") and not result["objectives_met"]:
                    failed.add(phase)
                    if phase == "high_cardinality" or any(s["OOMKilled"] for s in result["containers_after_phase"].values()):
                        # A deliberately overloaded dataset is evidence of
                        # saturation. Later phases use the last passing size.
                        (args.output / f"{name}.containers.log").write_text(deployment.run("logs", "--no-color"))
                        deployment.run("down", "--volumes", "--remove-orphans", "--timeout", "5")
                        deployment.start()
                        per_request = report["dataset"]["series_per_request"][signal]
                        for _ in range(math.ceil(last_cardinality / per_request)):
                            path, body, content, _ = write_request(signal, next(deployment.sequence), last_cardinality)
                            status, _ = http(INGEST[signal], path, last_cardinality_tenant, body, content)
                            if not 200 <= status < 300:
                                raise RuntimeError(f"{signal} could not restore the passing dataset")
                        if not deployment.drain(signal)["recovered"]:
                            raise RuntimeError(f"{signal} restored dataset did not become durable")
                elif phase == "high_cardinality":
                    last_cardinality_tenant = tenant
                    last_cardinality = cardinality
            # Stop the durable consumer, append a backlog, then time catch-up.
            builder = f"{signal}-block-builder"
            prior = deployment.drain(signal)["status"]
            deployment.run("stop", "--timeout", "120", builder)
            for _ in range(10):
                path, body, content, _ = write_request(signal, next(deployment.sequence), 100)
                status, _ = http(INGEST[signal], path, body=body, content_type=content)
                if not 200 <= status < 300:
                    raise RuntimeError(f"{signal} could not append the restart backlog: {status}")
            before = time.monotonic()
            deployment.run("start", builder)
            deployment.refresh_pids()
            catchup = deployment.drain(signal)
            before_offsets = [p["consumed_offset"] for c in prior["wal_consumers"] for p in c["partitions"] if p["assigned"]]
            after_offsets = [p["consumed_offset"] for c in (catchup["status"] or {}).get("wal_consumers", []) for p in c["partitions"] if p["assigned"]]
            appended_records = 10 * report["dataset"]["series_per_request"][signal] * (1 if signal == "profiles" else POINTS)
            recovered = catchup["recovered"] and len(before_offsets) == len(after_offsets) == 1 and (
                after_offsets[0] >= before_offsets[0] + appended_records)
            report["entries"].append({"signal": signal, "phase": "restart",
                                      "backlog_batches": 10, "recovered": recovered,
                                      "recovery_seconds": time.monotonic() - before,
                                      "before_stop": prior, "after_restart": catchup["status"],
                                      "telemetry": deployment.sample(signal)})
            if not recovered:
                raise RuntimeError(f"{signal} did not catch up after restart")
    finally:
        (args.output / "deployment-report.json").write_text(json.dumps(report, indent=2))
        deployment.close()


def self_test():
    records = [{"status": 204, "seconds": 0.1, "rows": 100},
               {"status": 429, "seconds": 0.2, "rows": 100}]
    result = summarize(records, 2)
    assert result["accepted_rows_per_sec"] == 50 and result["error_rate"] == 0.5
    assert quantiles([0.3, 0.1, 0.2]) == {"count": 3, "p50": 0.2, "p95": 0.3, "p99": 0.3}
    assert not has_data("metrics", b'{"status":"success","data":{"result":[]}}')
    assert has_data("logs", b'{"status":"success","data":{"result":[{}]}}')
    for signal in SIGNALS:
        path, body, content, rows = write_request(signal, 1, 100)
        assert rows == (10 if signal == "profiles" else 100 if signal == "traces" else 1000)
        assert path and body and content
    assert prometheus('# comment\nx{label="a"} 3\nx_invalid garbage\n') == {'x{label="a"}': 3}
    status = {"ready": True, "wal_consumers": [{"caught_up": True, "partitions": [
        {"assigned": True, "consumed_offset": 99, "committed_offset": 99}]}]}
    assert not durably_caught_up(status)  # Readiness alone does not prove a durable append.
    status["wal_consumers"][0]["partitions"][0]["committed_offset"] = 100
    assert durably_caught_up(status)
    # Three independently measured rates have a median of 200; one outlier
    # must not shift the baseline. Use a model report, not the HTTP generator.
    reports = []
    for index, rate in enumerate((180, 200, 220)):
        report = {key: {} for key in REFERENCE_FIELDS}
        report.update(schema_version=1, phase_seconds=60, run_id=str(index), image_commit="a" * 40,
                      image_digest="sha256:" + "b" * 64, harness_commit="c" * 40, entries=[])
        for signal in SIGNALS:
            for phase, level, met in (("steady", 0, True), ("burst", 1, True), ("burst", 2, False),
                                      ("high_cardinality", 100, True), ("high_cardinality", 1000, False),
                                      ("cold_window", 0, True), ("noisy_tenant", 0, True), ("restart", 0, True)):
                report["entries"].append({"signal": signal, "phase": phase, "objectives_met": met,
                    "writers": level, "cardinality": level, "recovered": True, "recovery_seconds": 1,
                    "ingest": {"attempts": 10, "durable_rows_per_sec": rate, "latency_seconds": {"p99": 0.1}},
                    "query": {"attempts": 10, "latency_seconds": {"p99": 0.1}},
                    "telemetry": {"rss_kib_peak_by_role": {"querier": 1000}, "object_requests": 20}})
        reports.append(report)
    baseline = qualification(reports)
    assert baseline["limits"]["logs"]["burst"]["level"] == 1
    assert baseline["metrics"]["metrics/steady/durable_rows_per_sec"]["median"] == 200
    qualification(reports, baseline)
    for mutate in (
        lambda rs: rs[1].update(run_id="0"),
        lambda rs: rs[1].update(host={"cpu_count": 8}),
        lambda rs: rs[0]["entries"][0]["telemetry"]["rss_kib_peak_by_role"].update(querier=5000),
        lambda rs: rs[0]["entries"][0]["ingest"].update(durable_rows_per_sec=10),
        lambda rs: rs[0]["entries"][0]["query"]["latency_seconds"].update(p99=0.4),
    ):
        changed = json.loads(json.dumps(reports))
        mutate(changed)
        try:
            qualification(changed, baseline)
        except ValueError:
            pass
        else:
            raise AssertionError("an invalid or regressed report passed qualification")
    print("deployment-envelope self-test passed")


REFERENCE_FIELDS = ("schema_version", "seed", "phase_seconds", "shape", "dataset", "objectives",
                    "host", "burst_levels", "cardinality_levels")


def qualification(reports, baseline=None):
    """Compute conservative limits, or compare the same load with its baseline."""
    if len(reports) < 3 or len({r["run_id"] for r in reports}) != len(reports):
        raise ValueError("qualification needs at least three distinct runs")
    first = reports[0]
    reference = {key: first[key] for key in REFERENCE_FIELDS}
    if first["phase_seconds"] < 60:
        raise ValueError("qualification needs at least 60 measured seconds per phase")
    for report in reports:
        for key in (*REFERENCE_FIELDS, "image_commit", "image_digest", "harness_commit"):
            if report[key] != first[key]:
                raise ValueError(f"qualification runs differ in {key}")
        if not re.fullmatch(r"[0-9a-f]{40}", report["image_commit"] or ""):
            raise ValueError("qualification requires the image source commit")
    if baseline and reference != baseline["reference"]:
        raise ValueError("workload or host differs from the baseline; review new measurements")
    limits, metrics = {}, {}
    for signal in SIGNALS:
        entries = [[e for e in report["entries"] if e["signal"] == signal] for report in reports]
        for run in entries:
            for phase in ("steady", "cold_window", "noisy_tenant", "restart"):
                found = [e for e in run if e["phase"] == phase]
                if len(found) != 1 or not found[0].get("objectives_met", found[0].get("recovered")):
                    raise ValueError(f"{signal}/{phase} did not meet its objectives")
        limits[signal] = {}
        selected = []
        for phase, unit, configured in (("burst", "writers", WRITERS),
                                        ("high_cardinality", "cardinality", CARDINALITIES)):
            steps = [{e[unit]: e for e in run if e["phase"] == phase} for run in entries]
            if any(not run or len(run) != len([e for e in entries[i] if e["phase"] == phase])
                   for i, run in enumerate(steps)):
                raise ValueError(f"{signal}/{phase} has missing or duplicate steps")
            passing = [n for n in configured if all(n in run and run[n]["objectives_met"] for run in steps)]
            if not passing:
                raise ValueError(f"{signal}/{phase} has no passing load")
            level = max(passing)
            if baseline:
                level = baseline["limits"][signal][phase]["level"]
                if any(level not in run or not run[level]["objectives_met"] for run in steps):
                    raise ValueError(f"{signal}/{phase} missed the published load {level}")
            failures = [n for n in configured if n > level and any(n in run and not run[n]["objectives_met"] for run in steps)]
            if not failures:
                raise ValueError(f"{signal}/{phase} did not reach measured saturation")
            limits[signal][phase] = {"level": level, "first_failing_level": min(failures), "unit": unit}
            selected.append((phase, [run[level] for run in steps]))
        for phase in ("steady", "cold_window", "noisy_tenant", "restart"):
            selected.append((phase, [next(e for e in run if e["phase"] == phase) for run in entries]))
        for phase, samples in selected:
            values = {}
            if phase == "restart":
                values["recovery_seconds"] = ("higher", [e["recovery_seconds"] for e in samples])
            else:
                values["query_p99_seconds"] = ("higher", [e["query"]["latency_seconds"]["p99"] for e in samples])
                values["peak_rss_kib"] = ("higher", [sum(e["telemetry"]["rss_kib_peak_by_role"].values()) for e in samples])
                values["object_requests_per_operation"] = ("higher", [e["telemetry"]["object_requests"] /
                    (e["ingest"]["attempts"] + e["query"]["attempts"]) for e in samples])
                if phase != "cold_window":
                    values["durable_rows_per_sec"] = ("lower", [e["ingest"]["durable_rows_per_sec"] for e in samples])
                    values["write_p99_seconds"] = ("higher", [e["ingest"]["latency_seconds"]["p99"] for e in samples])
            for metric, (direction, numbers) in values.items():
                if any(not isinstance(n, (float, int)) or not math.isfinite(n) or n < 0 for n in numbers):
                    raise ValueError(f"{signal}/{phase}/{metric} is not a finite nonnegative number")
                mean = statistics.fmean(numbers)
                name = f"{signal}/{phase}/{metric}"
                metrics[name] = {"direction": direction, "median": statistics.median(numbers),
                                 "minimum": min(numbers), "maximum": max(numbers), "mean": mean,
                                 "cv": statistics.pstdev(numbers) / mean if mean else 0}
                if baseline:
                    allowed = baseline["metrics"][name]["median"]
                    if direction == "lower" and min(numbers) < allowed / 1.5 or (
                        direction == "higher" and max(numbers) > max(allowed * 1.5,
                            2 if metric == "recovery_seconds" else 0.02 if metric.endswith("p99_seconds") else 1)):
                        raise ValueError(f"{name}: {numbers} regressed against {allowed} (1.5x tolerance)")
    return {"reference": reference, "limits": limits, "metrics": metrics,
            "run_ids": [r["run_id"] for r in reports], "image_commit": first["image_commit"],
            "image_digest": first["image_digest"], "harness_commit": first["harness_commit"]}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", help="Content-addressed Krabka image")
    parser.add_argument("--image-digest", help="OCI manifest digest for an image built locally")
    parser.add_argument("--image-commit", help="Source commit of the measured image")
    parser.add_argument("--reports", nargs="+", type=pathlib.Path, help="Qualify complete independent reports")
    parser.add_argument("--record", type=pathlib.Path, help="Write the measured qualification baseline")
    parser.add_argument("--baseline", type=pathlib.Path, help="Apply a recorded stable-runner baseline")
    parser.add_argument("--output", type=pathlib.Path, default=pathlib.Path("qualification/evidence/deployment"))
    parser.add_argument("--seconds", type=int, default=60)
    parser.add_argument("--self-test", action="store_true")
    options = parser.parse_args()
    if options.self_test:
        self_test()
    elif options.reports:
        try:
            result = qualification([json.loads(p.read_text()) for p in options.reports],
                                   json.loads(options.baseline.read_text()) if options.baseline else None)
            if options.record:
                options.record.write_text(json.dumps(result, indent=2) + "\n")
            else:
                print(json.dumps(result, indent=2))
        except (ValueError, KeyError, OSError) as error:
            parser.exit(1, f"deployment qualification failed: {error}\n")
    else:
        digest = options.image_digest or (options.image or "").split("@")[-1]
        if not options.image or not re.fullmatch(r"sha256:[0-9a-f]{64}", digest) or options.seconds <= 0:
            parser.error("--image needs an immutable digest and --seconds must be positive")
        run(options)
