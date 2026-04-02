"""Shared pytest configuration: Airflow env defaults + 'Unit tests:' summary prefix."""
from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

DAGS_FOLDER = Path(__file__).resolve().parent.parent / "dags"

# ---------------------------------------------------------------------------
# Pytest hook: Airflow env
# ---------------------------------------------------------------------------


def pytest_configure(config) -> None:  # noqa: ARG001
    """Set Airflow env vars so DAGs and utils import without a live DB."""
    os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "false")
    os.environ.setdefault("AIRFLOW__CORE__UNIT_TEST_MODE", "true")
    os.environ.setdefault(
        "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN",
        "sqlite:////tmp/airflow_unit_test.db",
    )
    os.environ.setdefault("AIRFLOW__CORE__DAGS_FOLDER", str(DAGS_FOLDER))
    _patch_terminal_summary_line()


# ---------------------------------------------------------------------------
# Terminal summary: prefix the final stats line with "Unit tests:"
# ---------------------------------------------------------------------------

_DURATION_TAIL = re.compile(r" in [\d.]+\s*s\s*$", re.IGNORECASE)
_OUTCOME_WORD = re.compile(
    r"\d+\s+(passed|failed|skipped|warnings?|errors?)\b", re.IGNORECASE,
)


def _is_pytest_summary_line(msg: str) -> bool:
    s = msg.strip()
    return bool(s and _DURATION_TAIL.search(s) and _OUTCOME_WORD.search(s))


def _patch_terminal_summary_line() -> None:
    from _pytest.terminal import TerminalReporter

    if getattr(TerminalReporter.write_line, "_patched", False):
        return

    _orig = TerminalReporter.write_line

    def write_line(self: Any, msg: str = "", **kwargs: Any) -> None:
        if isinstance(msg, str) and "Unit tests:" not in msg:
            if _is_pytest_summary_line(msg):
                msg = f"Unit tests: {msg.strip()}"
        return _orig(self, msg, **kwargs)

    write_line._patched = True  # type: ignore[attr-defined]
    TerminalReporter.write_line = write_line  # type: ignore[method-assign]
