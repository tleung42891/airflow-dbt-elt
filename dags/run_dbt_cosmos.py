"""Cosmos dbt DAGs — one Airflow task per warehouse model/test via Astronomer Cosmos.

Selection is parse-time (``RenderConfig``), so tag-scoped and full-project runs are
separate DAG ids (Cosmos cannot honor a runtime ``params.select`` the way ``run_dbt`` does):

- ``run_dbt_cosmos`` — full warehouse (``path:models``)
- ``run_dbt_cosmos_pulls`` — ``tag:pulls+`` (triggered by PR ingestion)
- ``run_dbt_cosmos_contributions`` — ``tag:contributions+`` (triggered by contributions ingestion)

Uses ``ExecutionMode.LOCAL``. Optional post-steps: Elementary, ``drop_stale_relations``.

dbt packages are installed during ``airflow-init`` (``dbt deps`` into the mounted
``dbt_project/``). Per-task ``install_deps`` is disabled on purpose.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Sequence

import pendulum
from airflow.operators.python import ShortCircuitOperator
from cosmos import (
    DbtDag,
    ExecutionConfig,
    ExecutionMode,
    LoadMode,
    ProfileConfig,
    ProjectConfig,
    RenderConfig,
)
from cosmos.constants import TestBehavior
from cosmos.operators.local import DbtRunLocalOperator, DbtRunOperationLocalOperator
from cosmos.profiles import PostgresUserPasswordProfileMapping

POSTGRES_CONN_ID = "postgres_default"

DBT_PROJECT_PATH = Path(
    os.environ.get(
        "DBT_PROJECT_PATH",
        Path(__file__).resolve().parent.parent / "dbt_project",
    )
)
DBT_EXECUTABLE_PATH = os.environ.get(
    "DBT_EXECUTABLE_PATH",
    "/opt/airflow/dbt_venv/bin/dbt",
)

profile_config = ProfileConfig(
    profile_name="postgres",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id=POSTGRES_CONN_ID,
        profile_args={"schema": "public"},
        disable_event_tracking=True,
    ),
)

execution_config = ExecutionConfig(
    execution_mode=ExecutionMode.LOCAL,
    dbt_executable_path=DBT_EXECUTABLE_PATH,
)


def _param_enabled(param_name: str):
    def _check(**context) -> bool:
        value = context.get("params", {}).get(param_name, False)
        if isinstance(value, str):
            return value.strip().lower() in {"1", "true", "yes", "y"}
        return bool(value)

    return _check


def _attach_post_steps(dag: DbtDag) -> None:
    """Wire optional Elementary + drop_stale_relations after Cosmos leaf tasks."""
    leaves = list(dag.leaves)

    check_elementary = ShortCircuitOperator(
        task_id="check_elementary",
        python_callable=_param_enabled("elementary"),
        dag=dag,
    )
    run_elementary = DbtRunLocalOperator(
        task_id="run_elementary",
        project_dir=DBT_PROJECT_PATH,
        profile_config=profile_config,
        dbt_executable_path=DBT_EXECUTABLE_PATH,
        select=["elementary"],
        install_deps=False,
        dag=dag,
    )
    check_drop_stale = ShortCircuitOperator(
        task_id="check_drop_stale_relations",
        python_callable=_param_enabled("drop_stale_relations"),
        dag=dag,
    )
    drop_stale = DbtRunOperationLocalOperator(
        task_id="drop_stale_relations",
        project_dir=DBT_PROJECT_PATH,
        profile_config=profile_config,
        dbt_executable_path=DBT_EXECUTABLE_PATH,
        macro_name="drop_stale_relations",
        args={"schema_name": "public", "dry_run": False},
        install_deps=False,
        dag=dag,
    )

    for leaf in leaves:
        leaf >> check_elementary
        leaf >> check_drop_stale

    check_elementary >> run_elementary
    check_drop_stale >> drop_stale


def create_cosmos_dbt_dag(dag_id: str, select: Sequence[str], *, extra_tags: Sequence[str] = ()) -> DbtDag:
    """Build a Cosmos DbtDag with a fixed parse-time selector."""
    # package:elementary must be excluded — path:models also matches package-internal
    # models/edr/... paths and balloons the graph (OOM risk on a laptop Celery worker).
    # dbt_deps must match operator_args['install_deps'] for LoadMode.DBT_LS + LOCAL.
    render_config = RenderConfig(
        load_method=LoadMode.DBT_LS,
        select=list(select),
        exclude=["package:elementary"],
        dbt_deps=False,
        # One dbt test after all model runs (dbt-like), not a .test task per model.
        test_behavior=TestBehavior.AFTER_ALL,
    )

    dag = DbtDag(
        dag_id=dag_id,
        schedule=None,
        start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
        catchup=False,
        max_active_runs=1,
        # Cap parallel Cosmos tasks — each LOCAL task is a full dbt process.
        max_active_tasks=4,
        tags=["dbt", "cosmos", *extra_tags],
        render_template_as_native_obj=True,
        params={
            "full_refresh": False,
            "elementary": False,
            "drop_stale_relations": False,
        },
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=profile_config,
        render_config=render_config,
        execution_config=execution_config,
        operator_args={
            "install_deps": False,
            "full_refresh": "{{ params.full_refresh }}",
        },
    )
    _attach_post_steps(dag)
    return dag


# Full warehouse rebuild (manual / on-demand).
run_dbt_cosmos = create_cosmos_dbt_dag(
    "run_dbt_cosmos",
    ["path:models"],
    extra_tags=["full"],
)

# Tag-scoped graphs for ingestion triggers (same selectors as former run_dbt conf).
run_dbt_cosmos_pulls = create_cosmos_dbt_dag(
    "run_dbt_cosmos_pulls",
    ["tag:pulls+"],
    extra_tags=["pulls"],
)

run_dbt_cosmos_contributions = create_cosmos_dbt_dag(
    "run_dbt_cosmos_contributions",
    ["tag:contributions+"],
    extra_tags=["contributions"],
)
