#!/usr/bin/env python3
"""Run each tests/test_*.py with pytest in sequence.

Prints one stats line on success. On failure, prints an ``Errors:`` section: for
each failing file, the relative path plus pytest's captured output (same shape as
``FAILURES`` / traceback when using ``--tb=short``).
"""
from __future__ import annotations

import io
import os
import re
import sys
import time
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from typing import Any, NamedTuple

_DOCKER_HINT = (
    "docker compose --profile tests run --rm -T -w /repo "
    "--entrypoint python3 dag-tests scripts/run_all_tests.py"
)
_MAX_LONG_MESSAGE = 500
_MAX_FAILURE_OUTPUT_CHARS = 120_000

_PROGRESS_LINE = re.compile(r"^[.\sFExs]+\[\s*\d+%\]\s*$", re.MULTILINE)
_STATS_LINE = re.compile(
    r"^(?:Unit tests:\s*)?\d.*(?:passed|failed|skipped).* in [\d.]+s\s*$",
    re.MULTILINE,
)


def _strip_progress(text: str) -> str:
    """Remove pytest's dot-progress bar and trailing stats line from captured output."""
    text = _PROGRESS_LINE.sub("", text)
    text = _STATS_LINE.sub("", text)
    return text.strip()


def _squash_multiline(text: str, limit: int) -> str:
    return text.strip().replace("\n", " ")[:limit]


def _failure_message(longrepr: Any) -> str:
    if longrepr is None:
        return "failed"
    try:
        rc = getattr(longrepr, "reprcrash", None)
        if rc is not None and getattr(rc, "message", None):
            return _squash_multiline(str(rc.message), _MAX_LONG_MESSAGE)
    except Exception:
        pass
    return _squash_multiline(str(longrepr), _MAX_LONG_MESSAGE)


class _SessionRecorder:
    """Pytest plugin: counts outcomes and collects short failure lines."""

    def __init__(self) -> None:
        self.passed = 0
        self.failed = 0
        self.skipped = 0
        self.errors: list[str] = []

    def pytest_collectreport(self, report: Any) -> None:
        if report.outcome != "failed":
            return
        self.failed += 1
        node = getattr(report, "nodeid", None) or "collection"
        msg = _failure_message(getattr(report, "longrepr", None))
        self.errors.append(f"{node} (collect) — {msg}")

    def pytest_runtest_logreport(self, report: Any) -> None:
        if report.when == "call":
            if report.passed:
                self.passed += 1
                return
            if report.skipped:
                self.skipped += 1
                return
        if not report.failed:
            return
        self.failed += 1
        phase = "" if report.when == "call" else f" [{report.when}]"
        self.errors.append(f"{report.nodeid}{phase} — {_failure_message(report.longrepr)}")


class _SuiteRun(NamedTuple):
    exit_code: int
    recorder: _SessionRecorder
    elapsed_s: float
    captured: str


def _run_suite(path: Path, pytest_main: Any) -> _SuiteRun:
    rec = _SessionRecorder()
    out, err = io.StringIO(), io.StringIO()
    t0 = time.perf_counter()
    with redirect_stdout(out), redirect_stderr(err):
        code = pytest_main([str(path), "-q", "--tb=short", "-rN"], plugins=[rec])
    elapsed = time.perf_counter() - t0
    raw = (out.getvalue() + err.getvalue()).strip()
    captured = _strip_progress(raw)
    return _SuiteRun(code, rec, elapsed, captured)


def _repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _discover_suites(root: Path) -> list[Path]:
    return sorted(root.glob("tests/test_*.py"))


def _apply_env_defaults() -> None:
    os.environ.setdefault("NO_COLOR", "1")
    os.environ.setdefault("PY_COLORS", "0")


def _format_failure_section(rel_path: str, captured: str, recorder: _SessionRecorder, exit_code: int) -> str:
    text = captured.strip()
    if not text:
        if recorder.errors:
            text = "\n".join(recorder.errors)
        else:
            text = f"pytest exit {exit_code} (no output captured)"
    if len(text) > _MAX_FAILURE_OUTPUT_CHARS:
        text = text[:_MAX_FAILURE_OUTPUT_CHARS] + "\n… (truncated)"
    return f"{rel_path}\n{text}"


def _print_failure_report(sections: list[str]) -> None:
    print("\nErrors:", file=sys.stdout)
    first = True
    for block in sections:
        if not first:
            print(file=sys.stdout)
        first = False
        print(block, file=sys.stdout)


def main() -> int:
    root = _repo_root()
    os.chdir(root)
    _apply_env_defaults()

    try:
        import pytest as pytest_mod  # type: ignore[import-untyped]
    except ImportError:
        print(
            "pytest is not installed. From repo root:\n  " + _DOCKER_HINT,
            file=sys.stderr,
        )
        return 1

    suites = _discover_suites(root)
    if not suites:
        print(f"No tests matching tests/test_*.py under {root}", file=sys.stderr)
        return 1

    total_passed = total_failed = total_skipped = 0
    wall = 0.0
    failure_sections: list[str] = []
    any_failure = False

    for path in suites:
        run = _run_suite(path, pytest_mod.main)
        wall += run.elapsed_s
        total_passed += run.recorder.passed
        total_failed += run.recorder.failed
        total_skipped += run.recorder.skipped

        if run.exit_code != 0:
            any_failure = True
            rel = str(path.relative_to(root))
            failure_sections.append(
                _format_failure_section(rel, run.captured, run.recorder, run.exit_code)
            )

    stats = (
        f"Suites: {len(suites)} | Tests: {total_passed} passed, {total_failed} failed, "
        f"{total_skipped} skipped | wall {wall:.2f}s"
    )
    print(stats, file=sys.stdout)
    if any_failure:
        _print_failure_report(failure_sections)
    sys.stdout.flush()
    return 1 if any_failure else 0


if __name__ == "__main__":
    raise SystemExit(main())
