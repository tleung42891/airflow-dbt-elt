"""Cosmos dbt DAG — full warehouse graph with runtime tag-based skipping.

The Airflow UI always shows **all** models plus ``TestBehavior.AFTER_ALL`` tests
(``path:models``, excluding ``package:elementary``). Upstream ingestion DAGs
trigger this DAG with ``conf["select"]`` (e.g. ``tag:pulls+``); tasks outside
that selection raise ``AirflowSkipException`` so only the tagged subgraph runs.

Empty / omitted ``select`` = run everything (manual full rebuild).

Uses ``ExecutionMode.LOCAL``. Optional post-steps: Elementary, ``drop_stale_relations``.

dbt packages are installed during ``airflow-init`` (``dbt deps`` into the mounted
``dbt_project/``). Per-task ``install_deps`` is disabled on purpose.
"""
from __future__ import annotations

import os
import re
from collections import defaultdict
from pathlib import Path
from typing import Any, Mapping, Optional, Set

import pendulum
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.utils.trigger_rule import TriggerRule
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
from cosmos.operators.local import (
    DbtRunLocalOperator,
    DbtRunOperationLocalOperator,
    DbtTestLocalOperator,
)
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

# tag:pulls+  /  tag:contributions  (optional trailing graph operator)
_TAG_SELECT_RE = re.compile(r"^tag:([^\s+]+)(\+?)$")

# Resolve / post-step task ids — never skip these via model selection.
_CONTROL_TASK_IDS = frozenset(
    {
        "resolve_selection",
        "check_elementary",
        "run_elementary",
        "check_drop_stale_relations",
        "drop_stale_relations",
    }
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


def _conf_and_params(context: Mapping[str, Any]) -> tuple[dict, dict]:
    dag_run = context.get("dag_run")
    conf = (dag_run.conf if dag_run and dag_run.conf else None) or {}
    params = context.get("params") or {}
    return conf, params


def _runtime_select(context: Mapping[str, Any]) -> str:
    """Selector from trigger conf (preferred) or DAG params. Empty = full run."""
    conf, params = _conf_and_params(context)
    raw = conf.get("select", params.get("select", ""))
    if raw is None:
        return ""
    if isinstance(raw, (list, tuple)):
        return " ".join(str(s) for s in raw).strip()
    return str(raw).strip()


def _param_enabled(param_name: str):
    def _check(**context) -> bool:
        conf, params = _conf_and_params(context)
        value = conf.get(param_name, params.get(param_name, False))
        if isinstance(value, str):
            return value.strip().lower() in {"1", "true", "yes", "y"}
        return bool(value)

    return _check


def _model_nodes_from_dag(dag) -> dict[str, dict[str, Any]]:
    """Map dbt unique_id → {name, tags, depends_on} for Cosmos model tasks."""
    nodes: dict[str, dict[str, Any]] = {}
    for task in dag.tasks:
        extra = getattr(task, "extra_context", None) or {}
        cfg = extra.get("dbt_node_config") or {}
        if cfg.get("resource_type") != "model":
            continue
        uid = cfg.get("unique_id")
        if not uid:
            continue
        nodes[uid] = {
            "name": cfg.get("resource_name") or cfg.get("name") or task.task_id,
            "tags": set(cfg.get("tags") or []),
            "depends_on": list(cfg.get("depends_on") or []),
        }
    return nodes


def selected_model_names(select: str, nodes: Mapping[str, Mapping[str, Any]]) -> Optional[Set[str]]:
    """Resolve a runtime selector to model names, or None to run all.

    Supports empty / ``path:models`` (full) and ``tag:<name>`` / ``tag:<name>+``.
    """
    select = (select or "").strip()
    if not select or select in {"path:models", "*"}:
        return None

    match = _TAG_SELECT_RE.match(select)
    if not match:
        raise ValueError(
            f"Unsupported runtime select {select!r}. "
            "Use empty (full), path:models, tag:<name>, or tag:<name>+."
        )

    tag, plus = match.group(1), match.group(2) == "+"
    seeds = [uid for uid, info in nodes.items() if tag in info["tags"]]
    selected_uids: set[str] = set(seeds)

    if plus and seeds:
        children: dict[str, list[str]] = defaultdict(list)
        for uid, info in nodes.items():
            for parent in info["depends_on"]:
                if parent in nodes:
                    children[parent].append(uid)
        stack = list(seeds)
        while stack:
            current = stack.pop()
            for child in children.get(current, []):
                if child not in selected_uids:
                    selected_uids.add(child)
                    stack.append(child)

    return {nodes[uid]["name"] for uid in selected_uids if uid in nodes}


def _resolve_selection(**context) -> None:
    """Publish selected model names (or None=all) for downstream skip checks."""
    select = _runtime_select(context)
    nodes = _model_nodes_from_dag(context["dag"])
    selected = selected_model_names(select, nodes)
    ti = context["ti"]
    ti.xcom_push(key="runtime_select", value=select)
    ti.xcom_push(key="selected_models", value=None if selected is None else sorted(selected))
    if selected is None:
        print("Runtime select empty/full — running all Cosmos model tasks")
    else:
        print(f"Runtime select {select!r} → {len(selected)} model(s): {sorted(selected)}")


def _model_resource_name(task) -> str:
    extra = getattr(task, "extra_context", None) or {}
    cfg = extra.get("dbt_node_config") or {}
    if cfg.get("resource_name"):
        return str(cfg["resource_name"])
    select = getattr(task, "select", None)
    if isinstance(select, (list, tuple)) and select:
        return str(select[0])
    if select:
        return str(select)
    return task.task_id


def _install_runtime_skips(dag: DbtDag) -> None:
    """Skip model tasks outside conf select; scope AFTER_ALL test the same way."""

    def _skip_unselected_model(context: Mapping[str, Any], *, task) -> None:
        selected = context["ti"].xcom_pull(task_ids="resolve_selection", key="selected_models")
        if selected is None:
            return
        name = _model_resource_name(task)
        if name not in selected:
            raise AirflowSkipException(
                f"Skipping {name!r}: not in runtime select "
                f"{context['ti'].xcom_pull(task_ids='resolve_selection', key='runtime_select')!r}"
            )

    def _scope_after_all_test(context: Mapping[str, Any], *, task) -> None:
        select = context["ti"].xcom_pull(task_ids="resolve_selection", key="runtime_select") or ""
        if select:
            # List form matches RenderConfig / Cosmos CLI joining.
            task.select = [select]
            print(f"AFTER_ALL test scoped to --select {select}")

    for task in dag.tasks:
        if task.task_id in _CONTROL_TASK_IDS:
            continue

        if isinstance(task, DbtTestLocalOperator):
            task.trigger_rule = TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
            previous = task.pre_execute

            def pre_execute(context, *, _task=task, _prev=previous):
                if callable(_prev):
                    _prev(context)
                _scope_after_all_test(context, task=_task)

            task.pre_execute = pre_execute
            continue

        if isinstance(task, DbtRunLocalOperator) and task.task_id != "run_elementary":
            previous = task.pre_execute

            def pre_execute(context, *, _task=task, _prev=previous):
                if callable(_prev):
                    _prev(context)
                _skip_unselected_model(context, task=_task)

            task.pre_execute = pre_execute


def _attach_resolve_selection(dag: DbtDag) -> None:
    resolve = PythonOperator(
        task_id="resolve_selection",
        python_callable=_resolve_selection,
        dag=dag,
    )
    roots = [t for t in dag.tasks if t.task_id != "resolve_selection" and not t.upstream_list]
    for root in roots:
        resolve >> root


def _attach_post_steps(dag: DbtDag) -> None:
    """Wire optional Elementary + drop_stale_relations after Cosmos leaf tasks."""
    leaves = list(dag.leaves)

    check_elementary = ShortCircuitOperator(
        task_id="check_elementary",
        python_callable=_param_enabled("elementary"),
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
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
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
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


# package:elementary must be excluded — path:models also matches package-internal
# models/edr/... paths and balloons the graph (OOM risk on a laptop Celery worker).
# dbt_deps must match operator_args['install_deps'] for LoadMode.DBT_LS + LOCAL.
render_config = RenderConfig(
    load_method=LoadMode.DBT_LS,
    select=["path:models"],
    exclude=["package:elementary"],
    dbt_deps=False,
    # One dbt test after all model runs (dbt-like), not a .test task per model.
    test_behavior=TestBehavior.AFTER_ALL,
)

run_dbt_cosmos = DbtDag(
    dag_id="run_dbt_cosmos",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    # Cap parallel Cosmos tasks — each LOCAL task is a full dbt process.
    max_active_tasks=4,
    tags=["dbt", "cosmos"],
    render_template_as_native_obj=True,
    params={
        "select": "",
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
_attach_post_steps(run_dbt_cosmos)
_attach_resolve_selection(run_dbt_cosmos)
_install_runtime_skips(run_dbt_cosmos)
