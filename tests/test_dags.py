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
