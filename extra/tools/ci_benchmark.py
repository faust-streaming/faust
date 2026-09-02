#!/usr/bin/env python3
"""Benchmark the current Faust checkout against recent stable releases."""
from __future__ import annotations

import argparse
import json
import os
import pathlib
import shutil
import statistics
import subprocess
import sys
import tempfile
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any, Iterable

DEFAULT_REPO = "faust-streaming/faust"
DEFAULT_RELEASE_COUNT = 2
DEFAULT_MAX_REGRESSION = 1.25

BENCHMARK_CODE = r'''
from __future__ import annotations

import gc
import json
import platform
import statistics
import time
from importlib import metadata

from faust.types.tuples import Message, TP, tp_set_to_map


def version():
    for name in ("faust-streaming", "faust"):
        try:
            return metadata.version(name)
        except metadata.PackageNotFoundError:
            pass
    return "unknown"


def measure(fn, iterations, rounds=9, warmups=3):
    for _ in range(warmups):
        fn(iterations)
    samples = []
    for _ in range(rounds):
        gc.collect()
        started = time.perf_counter_ns()
        fn(iterations)
        samples.append((time.perf_counter_ns() - started) / iterations)
    return statistics.median(samples), statistics.pstdev(samples)


def message_create(iterations):
    for i in range(iterations):
        Message(
            "orders", i % 32, i,
            timestamp=1_700_000_000.0,
            timestamp_type=1,
            headers=None,
            key=b"key",
            value=b"value",
            checksum=None,
        )


def message_refcount(iterations):
    message = Message(
        "orders", 0, 0,
        timestamp=1_700_000_000.0,
        timestamp_type=1,
        headers=None,
        key=b"key",
        value=b"value",
        checksum=None,
    )
    for _ in range(iterations):
        message.incref()
        message.decref()


TPS = {TP(f"topic-{i % 64}", i % 48) for i in range(4096)}


def group_topic_partitions(iterations):
    for _ in range(iterations):
        tp_set_to_map(TPS)


benchmarks = [
    ("message_create", "ns/op", message_create, 80_000),
    ("message_refcount", "ns/op", message_refcount, 300_000),
    ("tp_set_to_map", "ns/call", group_topic_partitions, 4_000),
]
results = []
for name, unit, function, iterations in benchmarks:
    value, spread = measure(function, iterations)
    results.append({"name": name, "unit": unit, "value": value, "range": spread})

print(json.dumps({
    "python": platform.python_version(),
    "implementation": platform.python_implementation(),
    "faust_version": version(),
    "results": results,
}, sort_keys=True))
'''


@dataclass(frozen=True)
class Target:
    label: str
    install_spec: str
    commit: str
    kind: str


def run(cmd: list[str], *, cwd: pathlib.Path | None = None, env: dict[str, str] | None = None) -> str:
    completed = subprocess.run(
        cmd,
        cwd=cwd,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    if completed.returncode:
        print(completed.stdout, file=sys.stderr)
        raise subprocess.CalledProcessError(completed.returncode, cmd, output=completed.stdout)
    return completed.stdout.strip()


def git(*args: str, cwd: pathlib.Path) -> str:
    return run(["git", *args], cwd=cwd)


def github_release_tags(repo: str, count: int) -> list[str]:
    request = urllib.request.Request(
        f"https://api.github.com/repos/{repo}/releases?per_page={max(count * 3, count)}",
        headers={"Accept": "application/vnd.github+json", "User-Agent": "faust-ci-benchmark"},
    )
    token = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")
    if token:
        request.add_header("Authorization", f"Bearer {token}")
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            releases = json.loads(response.read().decode())
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
        print(f"::warning::Could not query GitHub releases: {exc}", file=sys.stderr)
        return []
    tags = []
    for release in releases:
        if release.get("draft") or release.get("prerelease"):
            continue
        tag = release.get("tag_name")
        if tag:
            tags.append(tag)
        if len(tags) == count:
            break
    return tags


def discover_release_tags(repo: str, count: int, cwd: pathlib.Path) -> list[str]:
    tags = github_release_tags(repo, count)
    if len(tags) < count:
        try:
            local_tags = git("tag", "--sort=-creatordate", cwd=cwd).splitlines()
        except subprocess.CalledProcessError:
            local_tags = []
        for tag in local_tags:
            if tag and tag not in tags:
                tags.append(tag)
            if len(tags) == count:
                break
    return tags[:count]


def commit_for_ref(ref: str, cwd: pathlib.Path) -> str:
    try:
        return git("rev-list", "-n", "1", ref, cwd=cwd)
    except subprocess.CalledProcessError:
        return ref


def safe_name(value: str) -> str:
    return "".join(ch if ch.isalnum() or ch in "._-" else "-" for ch in value)


def create_venv(path: pathlib.Path) -> pathlib.Path:
    subprocess.check_call([sys.executable, "-m", "venv", str(path)])
    return path / ("Scripts/python.exe" if os.name == "nt" else "bin/python")


def install_target(python: pathlib.Path, install_spec: str, cwd: pathlib.Path) -> None:
    env = os.environ.copy()
    env.update({"NO_CYTHON": "1", "PIP_DISABLE_PIP_VERSION_CHECK": "1", "PYTHONUNBUFFERED": "1"})
    run(
        [str(python), "-m", "pip", "install", "--upgrade", "pip", "setuptools", "wheel", "setuptools_scm[toml]"],
        cwd=cwd,
        env=env,
    )
    # Install runtime dependencies as well. The benchmark imports Faust normally,
    # and an isolated --no-deps install fails before measurements can start.
    run(
        [str(python), "-m", "pip", "install", "--no-build-isolation", install_spec],
        cwd=cwd,
        env=env,
    )


def run_target(target: Target, workdir: pathlib.Path, source_root: pathlib.Path) -> dict[str, Any]:
    python = create_venv(workdir / safe_name(target.label))
    install_target(python, target.install_spec, source_root)
    env = os.environ.copy()
    env.update({"PYTHONHASHSEED": "0", "PYTHONUNBUFFERED": "1"})
    output = run([str(python), "-c", BENCHMARK_CODE], env=env)
    payload = json.loads(output.splitlines()[-1])
    payload.update({"target": target.label, "commit": target.commit, "kind": target.kind})
    return payload


def rounded(value: float) -> float:
    return round(float(value), 3)


def action_rows(payloads: Iterable[dict[str, Any]], *, current_only: bool) -> list[dict[str, Any]]:
    rows = []
    for payload in payloads:
        if current_only and payload["target"] != "current":
            continue
        for result in payload["results"]:
            name = result["name"] if current_only else f"{payload['target']}/{result['name']}"
            rows.append({
                "name": name,
                "unit": result["unit"],
                "value": rounded(result["value"]),
                "range": rounded(result.get("range", 0.0)),
                "extra": "\n".join([
                    f"target: {payload['target']}",
                    f"kind: {payload['kind']}",
                    f"commit: {payload['commit']}",
                    f"faust: {payload['faust_version']}",
                    f"python: {payload['implementation']} {payload['python']}",
                ]),
            })
    return rows


def result_map(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {result["name"]: result for result in payload["results"]}


def regressions(payloads: list[dict[str, Any]], threshold: float) -> list[str]:
    current = result_map(payloads[0])
    failures = []
    for baseline in payloads[1:]:
        baseline_results = result_map(baseline)
        for name, current_result in current.items():
            ratio = current_result["value"] / baseline_results[name]["value"]
            if ratio > threshold:
                failures.append(f"{name} is {ratio:.2f}x slower than {baseline['target']}")
    return failures


def write_summary(path: pathlib.Path, payloads: list[dict[str, Any]], tags: list[str], failures: list[str], threshold: float) -> None:
    current = result_map(payloads[0])
    lines = [
        "# Faust performance benchmark", "",
        f"Current commit: `{payloads[0]['commit']}`",
        f"Release baselines: {', '.join(f'`{tag}`' for tag in tags) or '_none found_'}",
        f"Regression threshold: `{threshold:.2f}x`", "",
        "| Benchmark | Current | Baseline | Delta | Status |",
        "|---|---:|---:|---:|---|",
    ]
    for baseline in payloads[1:]:
        baseline_results = result_map(baseline)
        for name, current_result in current.items():
            base = baseline_results[name]
            ratio = current_result["value"] / base["value"]
            status = "❌ slower" if ratio > threshold else "✅ ok"
            lines.append(
                f"| {name} | {rounded(current_result['value'])} {current_result['unit']} | "
                f"{rounded(base['value'])} {base['unit']} (`{baseline['target']}`) | "
                f"{(ratio - 1) * 100:+.1f}% | {status} |"
            )
    if failures:
        lines.extend(["", "## Regressions", *[f"- {failure}" for failure in failures]])
    path.write_text("\n".join(lines) + "\n")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", default=os.environ.get("GITHUB_REPOSITORY", DEFAULT_REPO))
    parser.add_argument("--release-count", type=int, default=DEFAULT_RELEASE_COUNT)
    parser.add_argument("--max-regression", type=float, default=float(os.environ.get("PERF_MAX_REGRESSION") or DEFAULT_MAX_REGRESSION))
    parser.add_argument("--output-json", type=pathlib.Path, default=pathlib.Path("benchmark-results.json"))
    parser.add_argument("--current-output-json", type=pathlib.Path, default=pathlib.Path("benchmark-current.json"))
    parser.add_argument("--summary", type=pathlib.Path, default=pathlib.Path("benchmark-summary.md"))
    parser.add_argument("--keep-workdir", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = pathlib.Path.cwd()
    tags = discover_release_tags(args.repo, args.release_count, root)
    targets = [Target("current", ".", commit_for_ref("HEAD", root), "checkout")]
    targets.extend(Target(tag, f"git+https://github.com/{args.repo}.git@{tag}", commit_for_ref(tag, root), "release") for tag in tags)

    tempdir = pathlib.Path(tempfile.mkdtemp(prefix="faust-benchmark-"))
    payloads = []
    try:
        for target in targets:
            print(f"::group::Benchmark {target.label}")
            payload = run_target(target, tempdir, root)
            payloads.append(payload)
            for result in payload["results"]:
                print(f"{target.label}/{result['name']}: {rounded(result['value'])} {result['unit']}")
            print("::endgroup::")
    finally:
        if args.keep_workdir:
            print(f"Keeping benchmark workdir: {tempdir}")
        else:
            shutil.rmtree(tempdir, ignore_errors=True)

    failures = regressions(payloads, args.max_regression) if tags else []
    args.output_json.write_text(json.dumps(action_rows(payloads, current_only=False), indent=2, sort_keys=True) + "\n")
    args.current_output_json.write_text(json.dumps(action_rows(payloads, current_only=True), indent=2, sort_keys=True) + "\n")
    write_summary(args.summary, payloads, tags, failures, args.max_regression)
    for failure in failures:
        print(f"::error::{failure}")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
