#!/usr/bin/env python3
"""Run Faust CI performance benchmarks against current checkout and releases.

The script intentionally keeps the benchmark workload self-contained and free of
Kafka/network I/O so it can run reliably on shared GitHub Actions runners.
"""
from __future__ import annotations

import argparse
import json
import os
import pathlib
import shutil
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


def _version() -> str:
    for distribution_name in ("faust-streaming", "faust"):
        try:
            return metadata.version(distribution_name)
        except metadata.PackageNotFoundError:
            continue
    return "unknown"


def _time_per_iteration(fn, iterations: int, *, rounds: int = 9, warmups: int = 3):
    for _ in range(warmups):
        fn(iterations)
    measurements = []
    for _ in range(rounds):
        gc.collect()
        started = time.perf_counter_ns()
        fn(iterations)
        elapsed = time.perf_counter_ns() - started
        measurements.append(elapsed / iterations)
    return {
        "value": statistics.median(measurements),
        "range": statistics.pstdev(measurements) if len(measurements) > 1 else 0.0,
    }


def _message_create(iterations: int) -> None:
    for i in range(iterations):
        Message(
            "orders",
            i % 32,
            i,
            timestamp=1_700_000_000.0,
            timestamp_type=1,
            headers=None,
            key=b"key",
            value=b"value",
            checksum=None,
        )


def _message_refcount(iterations: int) -> None:
    message = Message(
        "orders",
        0,
        0,
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


_TPS = {TP(f"topic-{i % 64}", i % 48) for i in range(4096)}


def _tp_set_to_map(iterations: int) -> None:
    for _ in range(iterations):
        tp_set_to_map(_TPS)


benchmarks = [
    ("message_create", "ns/op", _message_create, 80_000),
    ("message_refcount", "ns/op", _message_refcount, 300_000),
    ("tp_set_to_map", "ns/call", _tp_set_to_map, 4_000),
]

results = []
for name, unit, fn, iterations in benchmarks:
    measurement = _time_per_iteration(fn, iterations)
    results.append(
        {
            "name": name,
            "unit": unit,
            "value": measurement["value"],
            "range": measurement["range"],
        }
    )

print(
    json.dumps(
        {
            "python": platform.python_version(),
            "implementation": platform.python_implementation(),
            "faust_version": _version(),
            "results": results,
        },
        sort_keys=True,
    )
)
'''


@dataclass(frozen=True)
class Target:
    label: str
    install_spec: str
    commit: str
    kind: str


def _run(
    cmd: list[str],
    *,
    cwd: pathlib.Path | None = None,
    env: dict[str, str] | None = None,
) -> str:
    completed = subprocess.run(
        cmd,
        cwd=cwd,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=True,
    )
    return completed.stdout.strip()


def _git(*args: str, cwd: pathlib.Path | None = None) -> str:
    return _run(["git", *args], cwd=cwd)


def _release_tags_from_github(repo: str, count: int, token: str | None) -> list[str]:
    request = urllib.request.Request(
        f"https://api.github.com/repos/{repo}/releases?per_page={max(count * 2, count)}",
        headers={
            "Accept": "application/vnd.github+json",
            "User-Agent": "faust-ci-benchmark",
        },
    )
    if token:
        request.add_header("Authorization", f"Bearer {token}")
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            releases = json.loads(response.read().decode())
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
        print(f"::warning::Could not read GitHub releases for {repo}: {exc}", file=sys.stderr)
        return []
    tags: list[str] = []
    for release in releases:
        if release.get("draft") or release.get("prerelease"):
            continue
        tag = release.get("tag_name")
        if isinstance(tag, str) and tag:
            tags.append(tag)
        if len(tags) >= count:
            break
    return tags


def _release_tags_from_git(count: int, cwd: pathlib.Path) -> list[str]:
    try:
        tags = _git("tag", "--sort=-creatordate", cwd=cwd).splitlines()
    except subprocess.CalledProcessError as exc:
        print(f"::warning::Could not read local git tags: {exc.stdout}", file=sys.stderr)
        return []
    return [tag for tag in tags if tag][:count]


def _discover_release_tags(repo: str, count: int, cwd: pathlib.Path) -> list[str]:
    token = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")
    tags = _release_tags_from_github(repo, count, token)
    if len(tags) < count:
        for tag in _release_tags_from_git(count, cwd):
            if tag not in tags:
                tags.append(tag)
            if len(tags) >= count:
                break
    return tags[:count]


def _commit_for_ref(ref: str, cwd: pathlib.Path) -> str:
    try:
        return _git("rev-list", "-n", "1", ref, cwd=cwd)
    except subprocess.CalledProcessError:
        return ref


def _safe_dir_name(label: str) -> str:
    return "".join(ch if ch.isalnum() or ch in "._-" else "-" for ch in label)


def _create_venv(path: pathlib.Path) -> pathlib.Path:
    subprocess.check_call([sys.executable, "-m", "venv", str(path)])
    if os.name == "nt":
        return path / "Scripts" / "python.exe"
    return path / "bin" / "python"


def _install_target(python: pathlib.Path, install_spec: str, cwd: pathlib.Path) -> None:
    base_env = os.environ.copy()
    base_env.update(
        {
            "NO_CYTHON": "1",
            "PIP_DISABLE_PIP_VERSION_CHECK": "1",
            "PYTHONUNBUFFERED": "1",
        }
    )
    subprocess.check_call(
        [
            str(python),
            "-m",
            "pip",
            "install",
            "--upgrade",
            "pip",
            "setuptools",
            "wheel",
            "setuptools_scm[toml]",
            "Cython>=3.0.0",
        ],
        cwd=cwd,
        env=base_env,
    )
    subprocess.check_call(
        [str(python), "-m", "pip", "install", "--no-deps", "--no-build-isolation", install_spec],
        cwd=cwd,
        env=base_env,
    )


def _run_target(target: Target, workdir: pathlib.Path, source_root: pathlib.Path) -> dict[str, Any]:
    venv_dir = workdir / _safe_dir_name(target.label)
    python = _create_venv(venv_dir)
    _install_target(python, target.install_spec, source_root)
    env = os.environ.copy()
    env.update({"PYTHONHASHSEED": "0", "PYTHONUNBUFFERED": "1"})
    output = _run([str(python), "-c", BENCHMARK_CODE], env=env)
    payload = json.loads(output.splitlines()[-1])
    payload.update({"target": target.label, "commit": target.commit, "kind": target.kind})
    return payload


def _round(value: float) -> float:
    return round(float(value), 3)


def _benchmark_action_rows(
    payloads: Iterable[dict[str, Any]],
    *,
    current_only: bool,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for payload in payloads:
        if current_only and payload["target"] != "current":
            continue
        for result in payload["results"]:
            metric_name = result["name"] if current_only else f"{payload['target']}/{result['name']}"
            rows.append(
                {
                    "name": metric_name,
                    "unit": result["unit"],
                    "value": _round(result["value"]),
                    "range": _round(result.get("range", 0.0)),
                    "extra": "\n".join(
                        [
                            f"target: {payload['target']}",
                            f"kind: {payload['kind']}",
                            f"commit: {payload['commit']}",
                            f"faust: {payload['faust_version']}",
                            f"python: {payload['implementation']} {payload['python']}",
                        ]
                    ),
                }
            )
    return rows


def _result_map(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {result["name"]: result for result in payload["results"]}


def _format_value(result: dict[str, Any]) -> str:
    return f"{_round(result['value'])} {result['unit']}"


def _write_summary(
    path: pathlib.Path,
    payloads: list[dict[str, Any]],
    release_tags: list[str],
    failures: list[str],
    max_regression: float,
) -> None:
    current = next(payload for payload in payloads if payload["target"] == "current")
    current_results = _result_map(current)
    lines = [
        "# Faust performance benchmark",
        "",
        f"Current commit: `{current['commit']}`",
        f"Release baselines: {', '.join(f'`{tag}`' for tag in release_tags) or '_none found_'}",
        f"Regression threshold: `{max_regression:.2f}x` slower than a release baseline",
        "",
        "| Benchmark | Current | Baseline | Delta | Status |",
        "|---|---:|---:|---:|---|",
    ]
    for payload in payloads:
        if payload["target"] == "current":
            continue
        baseline_results = _result_map(payload)
        for name, current_result in current_results.items():
            baseline_result = baseline_results[name]
            ratio = current_result["value"] / baseline_result["value"]
            delta = (ratio - 1.0) * 100.0
            status = "❌ slower" if ratio > max_regression else "✅ ok"
            lines.append(
                "| {name} | {current_value} | {baseline_value} (`{target}`) | {delta:+.1f}% | {status} |".format(
                    name=name,
                    current_value=_format_value(current_result),
                    baseline_value=_format_value(baseline_result),
                    target=payload["target"],
                    delta=delta,
                    status=status,
                )
            )
    lines.extend(["", "## Raw current results", "", "```json"])
    lines.append(json.dumps(_benchmark_action_rows([current], current_only=True), indent=2, sort_keys=True))
    lines.append("```")
    if failures:
        lines.extend(["", "## Regressions", ""])
        lines.extend(f"- {failure}" for failure in failures)
    path.write_text("\n".join(lines) + "\n")


def _find_regressions(payloads: list[dict[str, Any]], max_regression: float) -> list[str]:
    current = next(payload for payload in payloads if payload["target"] == "current")
    current_results = _result_map(current)
    failures: list[str] = []
    for payload in payloads:
        if payload["target"] == "current":
            continue
        baseline_results = _result_map(payload)
        for name, current_result in current_results.items():
            baseline_result = baseline_results[name]
            ratio = current_result["value"] / baseline_result["value"]
            if ratio > max_regression:
                failures.append(
                    f"{name} is {ratio:.2f}x slower than {payload['target']} "
                    f"({_format_value(current_result)} vs {_format_value(baseline_result)})"
                )
    return failures


def _float_env(name: str, default: float) -> float:
    value = os.environ.get(name)
    return float(value) if value else default


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", default=os.environ.get("GITHUB_REPOSITORY", DEFAULT_REPO))
    parser.add_argument("--release-count", type=int, default=DEFAULT_RELEASE_COUNT)
    parser.add_argument("--max-regression", type=float, default=_float_env("PERF_MAX_REGRESSION", DEFAULT_MAX_REGRESSION))
    parser.add_argument("--output-json", type=pathlib.Path, default=pathlib.Path("benchmark-results.json"))
    parser.add_argument("--current-output-json", type=pathlib.Path, default=pathlib.Path("benchmark-current.json"))
    parser.add_argument("--summary", type=pathlib.Path, default=pathlib.Path("benchmark-summary.md"))
    parser.add_argument("--keep-workdir", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    source_root = pathlib.Path.cwd()
    release_tags = _discover_release_tags(args.repo, args.release_count, source_root)
    current_commit = _commit_for_ref("HEAD", source_root)
    targets = [Target("current", ".", current_commit, "checkout")]
    targets.extend(
        Target(
            tag,
            f"git+https://github.com/{args.repo}.git@{tag}",
            _commit_for_ref(tag, source_root),
            "release",
        )
        for tag in release_tags
    )

    tempdir = pathlib.Path(tempfile.mkdtemp(prefix="faust-benchmark-"))
    payloads: list[dict[str, Any]] = []
    try:
        for target in targets:
            print(f"::group::Benchmark {target.label}")
            payload = _run_target(target, tempdir, source_root)
            payloads.append(payload)
            for result in payload["results"]:
                print(f"{target.label}/{result['name']}: {_format_value(result)}")
            print("::endgroup::")
    finally:
        if args.keep_workdir:
            print(f"Keeping benchmark workdir: {tempdir}")
        else:
            shutil.rmtree(tempdir, ignore_errors=True)

    failures = _find_regressions(payloads, args.max_regression) if release_tags else []
    args.output_json.write_text(
        json.dumps(_benchmark_action_rows(payloads, current_only=False), indent=2, sort_keys=True) + "\n"
    )
    args.current_output_json.write_text(
        json.dumps(_benchmark_action_rows(payloads, current_only=True), indent=2, sort_keys=True) + "\n"
    )
    _write_summary(args.summary, payloads, release_tags, failures, args.max_regression)

    if failures:
        print("::error::Performance regression threshold exceeded")
        for failure in failures:
            print(f"::error::{failure}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
