"""Cosmos dbt DAG — tests only, one Airflow task per individual dbt test.

POC for warehouses where materialization happens outside Airflow (e.g.
Snowflake **dynamic tables** refresh themselves in-warehouse): only dbt
*tests* appear as Airflow tasks, grouped by model. Cosmos has no native
per-test render mode, so the DAG is post-processed at parse time:

1. Build normally with ``TestBehavior.AFTER_EACH`` (task group per model
   containing ``run`` + aggregate ``test``).
2. Remove every ``DbtRunLocalOperator``, rewiring its upstream tasks directly
   to its downstream tasks — dbt lineage ordering between tests is preserved,
   no skipped "run" squares appear in the UI.
3. Split each model's aggregate ``.test`` task into one ``DbtTestLocalOperator``
   per individual dbt test (read from Cosmos's parsed ``dbt ls`` graph), placed
   in the model's task group so the UI shows e.g. the ``stg_github_pulls``
   group with six test squares, each running a scoped
   ``dbt test --select <base_test_name>`` (hash suffix stripped).

Models without any tests contribute no task at all — their edges collapse
through to downstream models' tests.

Runtime scoping mirrors ``run_dbt_cosmos``, which triggers this DAG after
every run, forwarding its own runtime ``select`` (e.g.
``{"select": "tag:pulls+"}``): a ``resolve_selection`` root task resolves the
tag selector to model names (reusing ``selected_model_names`` from
``run_dbt_cosmos``), and each test task's ``pre_execute`` skips it when its
model is not selected. Empty / omitted ``select`` = run every test.

dbt packages are installed during ``airflow-init`` (``dbt deps`` into the
mounted ``dbt_project/``). Per-task ``install_deps`` is disabled on purpose.
"""
from __future__ import annotations

import os
import re
from collections import defaultdict
from pathlib import Path
from typing import Any, Mapping

import pendulum
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
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
from cosmos.constants import DbtResourceType, TestBehavior
from cosmos.operators.local import DbtRunLocalOperator, DbtTestLocalOperator
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
    ),
)

execution_config = ExecutionConfig(
    execution_mode=ExecutionMode.LOCAL,
    dbt_executable_path=DBT_EXECUTABLE_PATH,
)

# package:elementary must be excluded — path:models also matches package-internal
# models/edr/... paths and balloons the graph (OOM risk on a laptop Celery worker).
# dbt_deps must match operator_args['install_deps'] for LoadMode.DBT_LS + LOCAL.
render_config = RenderConfig(
    load_method=LoadMode.DBT_LS,
    select=["path:models"],
    exclude=["package:elementary"],
    dbt_deps=False,
    # One <model>.test task per model — every dbt test is listed in the UI.
    test_behavior=TestBehavior.AFTER_EACH,
)


def _detach_and_remove_task(dag: DbtDag, task, rewire: bool) -> None:
    """Detach a task's edges and remove it from the DAG and its task group.

    Edits happen at DAG parse time (module top level) so the scheduler,
    webserver, and workers all see the same graph. With ``rewire=True`` the
    task's upstream tasks are connected directly to its downstream tasks.
    """
    upstream_ids = set(task.upstream_task_ids)
    downstream_ids = set(task.downstream_task_ids)

    for up_id in upstream_ids:
        dag.task_dict[up_id].downstream_task_ids.discard(task.task_id)
    for down_id in downstream_ids:
        dag.task_dict[down_id].upstream_task_ids.discard(task.task_id)

    if rewire:
        for up_id in upstream_ids:
            for down_id in downstream_ids:
                dag.task_dict[up_id].downstream_task_ids.add(down_id)
                dag.task_dict[down_id].upstream_task_ids.add(up_id)

    dag._remove_task(task.task_id)


def _remove_model_run_tasks(dag: DbtDag) -> None:
    """Drop every model ``run`` task, rewiring upstream tasks to downstream ones."""
    for task in [t for t in dag.tasks if isinstance(t, DbtRunLocalOperator)]:
        _detach_and_remove_task(dag, task, rewire=True)


def _split_model_test_tasks(dag: DbtDag) -> None:
    """Replace each aggregate ``<model>.test`` task with one task per dbt test.

    Cosmos keeps the parsed ``dbt ls`` graph on the DAG (``dag.dbt_graph``),
    including all test nodes and the model each one depends on. Each new task
    inherits the aggregate task's task group and upstream/downstream edges, so
    the per-model grouping and cross-model lineage ordering are unchanged.
    """
    nodes = dag.dbt_graph.filtered_nodes
    model_name_by_uid = {
        uid: node.name
        for uid, node in nodes.items()
        if node.resource_type == DbtResourceType.MODEL
    }

    tests_by_model: dict[str, list[str]] = defaultdict(list)
    for node in nodes.values():
        if node.resource_type != DbtResourceType.TEST or not node.depends_on:
            continue
        # For multi-parent tests (e.g. relationships) dbt lists the model the
        # test is defined on last — attach there to avoid duplicate tasks.
        model_name = model_name_by_uid.get(node.depends_on[-1])
        if model_name:
            tests_by_model[model_name].append(node.name)

    # old aggregate task_id -> new per-test task_ids, for fixing group-level refs
    replacements: dict[str, set[str]] = {}

    for task in [t for t in dag.tasks if isinstance(t, DbtTestLocalOperator)]:
        group = task.task_group
        model_name = group.group_id if group and group.group_id else task.task_id.removesuffix(".test")
        test_names = sorted(tests_by_model.get(model_name, []))
        if not test_names:
            continue

        upstream = list(task.upstream_list)
        downstream = list(task.downstream_list)
        _detach_and_remove_task(dag, task, rewire=False)

        new_ids: set[str] = set()
        used_labels: set[str] = set()
        for test_name in test_names:
            # Cosmos/dbt ls node names suffix long generic tests with
            # _<10-hex-char hash>. Strip it for readable task ids and for proper selections
            base_test_name = re.sub(r"_[0-9a-f]{10}$", "", test_name)
            label = base_test_name
            if label in used_labels:
                label = test_name
            used_labels.add(label)

            test_task = DbtTestLocalOperator(
                task_id=label,
                task_group=group,
                dag=dag,
                project_dir=DBT_PROJECT_PATH,
                profile_config=profile_config,
                dbt_executable_path=DBT_EXECUTABLE_PATH,
                select=[base_test_name],
                install_deps=False,
            )
            new_ids.add(test_task.task_id)
            for up in upstream:
                up >> test_task
            for down in downstream:
                test_task >> down
        replacements[task.task_id] = new_ids

    # Cosmos wires model dependencies as group >> group, which records the
    # upstream group's leaf task ids on the downstream TaskGroup. Swap the
    # removed aggregate ids for the new per-test ids so topological sort,
    # serialization, and collapsed-group edges stay consistent.
    def _walk(group: TaskGroup):
        yield group
        for child in group.children.values():
            if isinstance(child, TaskGroup):
                yield from _walk(child)

    for group in _walk(dag.task_group):
        for old_id, new_ids in replacements.items():
            for id_set in (group.upstream_task_ids, group.downstream_task_ids):
                if old_id in id_set:
                    id_set.discard(old_id)
                    id_set.update(new_ids)


def _model_selection_nodes(dag: DbtDag) -> dict[str, dict[str, Any]]:
    """dbt model unique_id → {name, tags, depends_on}, from Cosmos's dbt graph.

    Same shape ``run_dbt_cosmos._model_nodes_from_dag`` produces, but sourced
    from ``dag.dbt_graph`` since model tasks no longer exist in this DAG.
    """
    nodes = dag.dbt_graph.filtered_nodes
    return {
        uid: {
            "name": node.name,
            "tags": set(node.tags or []),
            "depends_on": list(node.depends_on or []),
        }
        for uid, node in nodes.items()
        if node.resource_type == DbtResourceType.MODEL
    }


def _resolve_selection(**context) -> None:
    """Publish selected model names (or None=all) for downstream skip checks."""
    # Deferred import: loading run_dbt_cosmos builds its whole Cosmos DAG,
    # which we only want to pay for at task runtime, not at every DAG parse.
    from run_dbt_cosmos import selected_model_names

    dag_run = context.get("dag_run")
    conf = (dag_run.conf if dag_run and dag_run.conf else None) or {}
    params = context.get("params") or {}
    raw = conf.get("select", params.get("select", "")) or ""
    if isinstance(raw, (list, tuple)):
        raw = " ".join(str(s) for s in raw)
    select = str(raw).strip()

    selected = selected_model_names(select, _model_selection_nodes(context["dag"]))
    ti = context["ti"]
    ti.xcom_push(key="runtime_select", value=select)
    ti.xcom_push(key="selected_models", value=None if selected is None else sorted(selected))
    if selected is None:
        print("Runtime select empty/full — running all test tasks")
    else:
        print(f"Runtime select {select!r} → {len(selected)} model(s): {sorted(selected)}")


def _install_runtime_skips(dag: DbtDag) -> None:
    """Skip test tasks whose model is outside the runtime ``conf['select']``."""

    def _skip_unselected(context: Mapping[str, Any]) -> None:
        selected = context["ti"].xcom_pull(task_ids="resolve_selection", key="selected_models")
        if selected is None:
            return
        model_name = context["task"].task_id.split(".", 1)[0]
        if model_name not in selected:
            raise AirflowSkipException(
                f"Skipping {context['task'].task_id!r}: model {model_name!r} not in "
                f"runtime select "
                f"{context['ti'].xcom_pull(task_ids='resolve_selection', key='runtime_select')!r}"
            )

    for task in dag.tasks:
        if task.task_id == "resolve_selection":
            continue
        # Skipped tests must not cascade: downstream tests fire (and decide
        # for themselves) as long as nothing has failed.
        task.trigger_rule = TriggerRule.NONE_FAILED
        previous = task.pre_execute

        def pre_execute(context, *, _prev=previous):
            if callable(_prev):
                _prev(context)
            _skip_unselected(context)

        task.pre_execute = pre_execute


def _attach_resolve_selection(dag: DbtDag) -> None:
    """Add the resolve task upstream of every root task group / task.

    Wired at *group* level (``resolve >> group``), not to the root tasks
    inside the groups: task-level edges alone leave the groups looking like
    roots in the UI's hierarchy ordering, which lists ``resolve_selection``
    after them instead of at the top.
    """
    resolve = PythonOperator(
        task_id="resolve_selection",
        python_callable=_resolve_selection,
        dag=dag,
    )
    for child in dag.task_group.children.values():
        if child.node_id == "resolve_selection":
            continue
        if isinstance(child, TaskGroup):
            if not child.upstream_task_ids and not child.upstream_group_ids:
                resolve >> child
        elif not child.upstream_list:
            resolve >> child


test_dbt_cosmos = DbtDag(
    dag_id="test_dbt_cosmos",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    # Cap parallel Cosmos tasks — each LOCAL task is a full dbt process.
    max_active_tasks=4,
    tags=["dbt", "cosmos", "tests-only"],
    params={"select": ""},
    project_config=ProjectConfig(DBT_PROJECT_PATH),
    profile_config=profile_config,
    render_config=render_config,
    execution_config=execution_config,
    operator_args={"install_deps": False},
)
_remove_model_run_tasks(test_dbt_cosmos)
_split_model_test_tasks(test_dbt_cosmos)
_install_runtime_skips(test_dbt_cosmos)
_attach_resolve_selection(test_dbt_cosmos)
