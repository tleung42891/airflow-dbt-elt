"""DAG load tests: ensure DAGs parse with no import errors and expected ids exist."""
from __future__ import annotations

from pathlib import Path

import pytest

pytest.importorskip("airflow")

from airflow.models import DagBag

_DAGS = Path(__file__).resolve().parent.parent / "dags"

EXPECTED_DAG_IDS = frozenset(
    {
        "github_to_postgres_and_dbt",
        "github_contributions_to_postgres_and_dbt",
        "run_dbt",
        "run_dbt_cosmos",
    }
)


@pytest.fixture
def dag_bag():
    return DagBag(dag_folder=str(_DAGS), include_examples=False)


def test_dag_bag_no_import_errors(dag_bag):
    assert dag_bag.import_errors == {}, f"Import errors: {dag_bag.import_errors}"


def test_expected_dags_present(dag_bag):
    missing = EXPECTED_DAG_IDS - frozenset(dag_bag.dag_ids)
    assert not missing, f"Missing DAGs: {missing}"


@pytest.mark.parametrize("dag_id", sorted(EXPECTED_DAG_IDS))
def test_dag_has_tasks(dag_bag, dag_id):
    # Use dag_bag.dags (parsed from files). get_dag() hits the metadata DB and needs migrations.
    dag = dag_bag.dags.get(dag_id)
    assert dag is not None
    assert len(dag.tasks) >= 1, f"{dag_id} should have at least one task"


@pytest.mark.parametrize("dag_id", sorted(EXPECTED_DAG_IDS))
def test_dag_has_no_cycles(dag_bag, dag_id):
    dag = dag_bag.dags.get(dag_id)
    assert dag is not None
    # topological_sort raises if there is a cycle
    list(dag.topological_sort())


def test_run_dbt_cosmos_has_post_steps(dag_bag):
    dag = dag_bag.dags.get("run_dbt_cosmos")
    assert dag is not None
    task_ids = set(dag.task_ids)
    assert "resolve_selection" in task_ids
    assert "check_elementary" in task_ids
    assert "run_elementary" in task_ids
    assert "check_drop_stale_relations" in task_ids
    assert "drop_stale_relations" in task_ids
    assert len(dag.tasks) > 4


def test_ingestion_triggers_cosmos_with_tag_select(dag_bag):
    pulls = dag_bag.dags["github_to_postgres_and_dbt"]
    contrib = dag_bag.dags["github_contributions_to_postgres_and_dbt"]
    pulls_trigger = pulls.get_task("trigger_run_dbt_cosmos")
    contrib_trigger = contrib.get_task("trigger_run_dbt_cosmos")
    assert pulls_trigger.trigger_dag_id == "run_dbt_cosmos"
    assert contrib_trigger.trigger_dag_id == "run_dbt_cosmos"
    assert pulls_trigger.conf["select"] == "tag:pulls+"
    assert contrib_trigger.conf["select"] == "tag:contributions+"


def test_selected_model_names_tag_plus():
    from run_dbt_cosmos import selected_model_names

    nodes = {
        "model.x.stg_pulls": {
            "name": "stg_github_pulls",
            "tags": {"pulls"},
            "depends_on": [],
        },
        "model.x.mart_pulls": {
            "name": "mart_pulls",
            "tags": set(),
            "depends_on": ["model.x.stg_pulls"],
        },
        "model.x.stg_contrib": {
            "name": "stg_github_contributions",
            "tags": {"contributions"},
            "depends_on": [],
        },
    }
    assert selected_model_names("", nodes) is None
    assert selected_model_names("tag:pulls+", nodes) == {"stg_github_pulls", "mart_pulls"}
    assert selected_model_names("tag:pulls", nodes) == {"stg_github_pulls"}
    assert selected_model_names("tag:contributions+", nodes) == {"stg_github_contributions"}
