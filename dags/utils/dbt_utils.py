"""
Utility functions for running dbt transformations in Airflow DAGs.
"""
from __future__ import annotations

from airflow.operators.bash import BashOperator
from typing import Optional, Tuple


def _full_refresh_flag(full_refresh: Optional[bool]) -> Tuple[str, str]:
    """Return (flag_init, inline_flag) for the --full-refresh flag.

    When ``full_refresh`` is None the flag is resolved at runtime from Airflow params.
    """
    if full_refresh is None:
        flag_init = """
                    # Check if full_refresh parameter is set to true
                    FULL_REFRESH_FLAG=""
                    if [ "{{ params.full_refresh | lower }}" = "true" ]; then
                        FULL_REFRESH_FLAG="--full-refresh"
                        echo "Running dbt with --full-refresh flag"
                    else
                        echo "Running dbt in incremental mode (no --full-refresh)"
                    fi
                    """
        return flag_init, "$FULL_REFRESH_FLAG"

    return "", "--full-refresh" if full_refresh else ""


def _select_flag(select: Optional[str]) -> Tuple[str, str]:
    """Return (flag_init, inline_flag) for the --select selector (e.g. 'tag:pulls').

    When ``select`` is None the selector is resolved at runtime from Airflow params.
    An empty string runs all models (no selector).
    """
    if select is None:
        flag_init = """
                    # Check if a select selector was provided
                    SELECT_FLAG=""
                    if [ -n "{{ params.select }}" ]; then
                        SELECT_FLAG="--select {{ params.select }}"
                        echo "Scoping dbt to selector: {{ params.select }}"
                    else
                        echo "Running dbt on all models"
                    fi
                    """
        return flag_init, "$SELECT_FLAG"

    if select:
        return "", f"--select {select}"

    return "", ""


def _elementary_flag(
    elementary: Optional[bool],
    docker_container: str,
    profiles_dir: str,
    project_dir: str,
) -> Tuple[str, str]:
    """Return (flag_init, trailing_cmd) for the elementary deployment step.

    When ``elementary`` is None the toggle is resolved at runtime from Airflow params.
    """
    run_cmd = (
        f"docker exec {docker_container} dbt run --select elementary "
        f"--profiles-dir {profiles_dir} --project-dir {project_dir}"
    )

    if elementary is None:
        flag_init = """
                    # Check if elementary parameter is set to true
                    RUN_ELEMENTARY=""
                    if [ "{{ params.elementary | lower }}" = "true" ]; then
                        RUN_ELEMENTARY="true"
                        echo "Will run elementary tests after dbt run and tests"
                    else
                        echo "Skipping elementary tests"
                    fi
                    """
        trailing = f"""
                    if [ "$RUN_ELEMENTARY" == "true" ]; then
                        echo "Running elementary tests..."
                        {run_cmd}
                    fi
                    """
        return flag_init, trailing

    if elementary:
        trailing = f"""
                    echo "Running elementary tests..."
                    {run_cmd}
                    """
        return "", trailing

    return "", ""


def _drop_stale_flag(
    drop_stale_relations: Optional[bool],
    docker_container: str,
    profiles_dir: str,
    project_dir: str,
) -> str:
    """Return the trailing command block for the drop_stale_relations macro.

    When ``drop_stale_relations`` is None the toggle is resolved at runtime from Airflow params.
    """
    run_op = (
        f"docker exec {docker_container} dbt run-operation drop_stale_relations "
        f"--args '{{\"schema_name\": \"public\", \"dry_run\": false}}' "
        f"--profiles-dir {profiles_dir} --project-dir {project_dir}"
    )

    if drop_stale_relations is None:
        return f"""
                    # Check if drop_stale_relations parameter is set to true
                    if [ "{{{{ params.drop_stale_relations | lower }}}}" = "true" ]; then
                        echo "Dropping stale relations via dbt macro..."
                        {run_op}
                    else
                        echo "Skipping drop_stale_relations macro"
                    fi
                    """

    if drop_stale_relations:
        return f"""
                    echo "Dropping stale relations via dbt macro..."
                    {run_op}
                    """

    return ""


def create_dbt_run_task(
    task_id: str = 'run_dbt_transformations',
    full_refresh: Optional[bool] = False,
    elementary: Optional[bool] = False,
    drop_stale_relations: Optional[bool] = False,
    select: Optional[str] = '',
    docker_container: str = 'dbt_cli',
    profiles_dir: str = '/usr/app/dbt',
    project_dir: str = '/usr/app/dbt'
) -> BashOperator:
    """
    Creates a BashOperator task to run dbt transformations.
    
    Args:
        task_id: The task ID for the BashOperator. Default: 'run_dbt_transformations'
        full_refresh: If True, runs dbt with --full-refresh flag. If False, runs incrementally.
                     If None, reads full_refresh from Airflow params context. Default: False
        elementary: If True, runs elementary tests after dbt run. If False, skips elementary tests.
                   If None, reads elementary from Airflow params context. Default: False
        drop_stale_relations: If True, runs the drop_stale_relations dbt macro after tests.
                             If None, reads drop_stale_relations from Airflow params context. Default: False
        select: dbt selector applied to `dbt run` and `dbt test` (e.g. 'tag:pulls'). Empty string
                runs all models. If None, reads the selector from Airflow params context. Default: ''
        docker_container: Name of the Docker container running dbt. Default: 'dbt_cli'
        profiles_dir: Path to dbt profiles directory in container. Default: '/usr/app/dbt'
        project_dir: Path to dbt project directory in container. Default: '/usr/app/dbt'
    
    Returns:
        BashOperator configured to run dbt transformations
    
    Examples:
        # Direct boolean value
        create_dbt_run_task(full_refresh=True, elementary=True)
        
        # Scope to a tag
        create_dbt_run_task(select='tag:pulls')
        
        # Use Airflow params
        create_dbt_run_task(full_refresh=None, elementary=None, select=None)  # Reads from {{ params.* }}
    """
    full_refresh_pre, full_refresh_var = _full_refresh_flag(full_refresh)
    select_pre, select_var = _select_flag(select)
    elementary_pre, elementary_cmd = _elementary_flag(
        elementary, docker_container, profiles_dir, project_dir
    )
    drop_stale_cmd = _drop_stale_flag(
        drop_stale_relations, docker_container, profiles_dir, project_dir
    )

    # flag_init holds the bash var initialization for any params-driven flags.
    flag_init = "".join([full_refresh_pre, elementary_pre, select_pre])

    # Single bash command template
    # Run dbt models, then regular tests, then elementary tests (if enabled), then drop stale relations (if enabled)
    bash_command = f"""
                    {flag_init}
                    echo "Running dbt models..."
                    docker exec {docker_container} dbt run {full_refresh_var} {select_var} --profiles-dir {profiles_dir} --project-dir {project_dir}
                    echo "Running dbt tests..."
                    docker exec {docker_container} dbt test {select_var} --profiles-dir {profiles_dir} --project-dir {project_dir}
                    {elementary_cmd}
                    {drop_stale_cmd}
                """

    return BashOperator(
        task_id=task_id,
        bash_command=bash_command.strip()
    )
