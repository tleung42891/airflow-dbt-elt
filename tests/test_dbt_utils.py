"""Unit tests for utils.dbt_utils (BashOperator command construction)."""
from __future__ import annotations

import pytest

pytest.importorskip("airflow")

from airflow.operators.bash import BashOperator

from utils.dbt_utils import create_dbt_run_task


def test_create_dbt_run_task_returns_bash_operator():
    op = create_dbt_run_task()
    assert isinstance(op, BashOperator)
    assert op.task_id == "run_dbt_transformations"


def test_bash_includes_dbt_run_and_test():
    op = create_dbt_run_task(full_refresh=False, elementary=False, drop_stale_relations=False)
    cmd = op.bash_command
    assert "dbt run" in cmd
    assert "dbt test" in cmd
    assert "docker exec dbt_cli" in cmd
    assert "--profiles-dir /usr/app/dbt" in cmd


def test_full_refresh_flag_when_true():
    op = create_dbt_run_task(full_refresh=True, elementary=False, drop_stale_relations=False)
    assert "--full-refresh" in op.bash_command


def test_elementary_true_uses_dbt_run_select_elementary():
    op = create_dbt_run_task(full_refresh=False, elementary=True, drop_stale_relations=False)
    cmd = op.bash_command
    assert "dbt run --select elementary" in cmd
    assert "dbt test --select elementary" not in cmd


def test_drop_stale_relations_true():
    op = create_dbt_run_task(
        full_refresh=False, elementary=False, drop_stale_relations=True
    )
    cmd = op.bash_command
    assert "drop_stale_relations" in cmd
    assert "dry_run" in cmd


def test_elementary_false_omits_elementary_command():
    op = create_dbt_run_task(full_refresh=False, elementary=False, drop_stale_relations=False)
    assert "select elementary" not in op.bash_command


def test_full_refresh_false_omits_flag():
    op = create_dbt_run_task(full_refresh=False, elementary=False, drop_stale_relations=False)
    assert "--full-refresh" not in op.bash_command


def test_custom_task_id():
    op = create_dbt_run_task(task_id="my_custom_dbt")
    assert op.task_id == "my_custom_dbt"


def test_custom_docker_container_in_command():
    op = create_dbt_run_task(docker_container="my_dbt_container")
    assert "docker exec my_dbt_container" in op.bash_command


def test_dbt_run_before_dbt_test():
    op = create_dbt_run_task(full_refresh=False, elementary=False, drop_stale_relations=False)
    cmd = op.bash_command
    assert cmd.index("dbt run") < cmd.index("dbt test")


def test_params_mode_includes_jinja_for_full_refresh():
    op = create_dbt_run_task(full_refresh=None, elementary=None, drop_stale_relations=None)
    cmd = op.bash_command
    assert "params.full_refresh" in cmd
    assert "params.elementary" in cmd
    assert "params.drop_stale_relations" in cmd


def test_select_applies_to_both_run_and_test():
    op = create_dbt_run_task(select="tag:pulls")
    cmd = op.bash_command
    # Applied to the dbt run and the dbt test commands
    assert cmd.count("--select tag:pulls") == 2


def test_select_params_mode_includes_jinja():
    op = create_dbt_run_task(select=None)
    assert "params.select" in op.bash_command


def test_select_empty_omits_flag():
    op = create_dbt_run_task(select="")
    assert "--select" not in op.bash_command
