"""
Utility functions for running dbt transformations in Airflow DAGs.
"""
from airflow.operators.bash import BashOperator
from typing import Optional

def create_dbt_run_task(
    task_id: str = 'run_dbt_transformations',
    full_refresh: Optional[bool] = False,
    elementary: Optional[bool] = False,
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
        docker_container: Name of the Docker container running dbt. Default: 'dbt_cli'
        profiles_dir: Path to dbt profiles directory in container. Default: '/usr/app/dbt'
        project_dir: Path to dbt project directory in container. Default: '/usr/app/dbt'
    
    Returns:
        BashOperator configured to run dbt transformations
    
    Examples:
        # Direct boolean value
        create_dbt_run_task(full_refresh=True, elementary=True)
        
        # Use Airflow params
        create_dbt_run_task(full_refresh=None, elementary=None)  # Reads from {{ params.full_refresh }} and {{ params.elementary }}
    """
    # Build the flag initialization logic conditionally
    flag_init_parts = []
    flag_var_parts = []
    
    # Handle full_refresh flag
    if full_refresh is None:
        # Use Airflow params template
        flag_init_parts.append("""
                    # Check if full_refresh parameter is set to true
                    FULL_REFRESH_FLAG=""
                    if [ "{{{{ params.full_refresh }}}}" == "True" ] || [ "{{{{ params.full_refresh }}}}" == "true" ]; then
                        FULL_REFRESH_FLAG="--full-refresh"
                        echo "Running dbt with --full-refresh flag"
                    else
                        echo "Running dbt in incremental mode (no --full-refresh)"
                    fi
                    """)
        flag_var_parts.append("$FULL_REFRESH_FLAG")
    else:
        # Use direct boolean value
        flag_value = "--full-refresh" if full_refresh else ""
        flag_var_parts.append(flag_value)
    
    # Handle elementary flag
    if elementary is None:
        # Use Airflow params template
        flag_init_parts.append("""
                    # Check if elementary parameter is set to true
                    RUN_ELEMENTARY=""
                    if [ "{{{{ params.elementary }}}}" == "True" ] || [ "{{{{ params.elementary }}}}" == "true" ]; then
                        RUN_ELEMENTARY="true"
                        echo "Will run elementary tests after dbt run and tests"
                    else
                        echo "Skipping elementary tests"
                    fi
                    """)
        elementary_test_cmd = f"""
                    if [ "$RUN_ELEMENTARY" == "true" ]; then
                        echo "Running elementary tests..."
                        docker exec {docker_container} dbt test --select elementary --profiles-dir {profiles_dir} --project-dir {project_dir}
                    fi
                    """
    else:
        # Use direct boolean value
        if elementary:
            elementary_test_cmd = f"""
                    echo "Running elementary tests..."
                    docker exec {docker_container} dbt run --select elementary --profiles-dir {profiles_dir} --project-dir {project_dir}
                    """
        else:
            elementary_test_cmd = ""
    
    # Combine flag initialization
    flag_init = "".join(flag_init_parts)
    flag_var = " ".join([f for f in flag_var_parts if f])  # Join non-empty flags
    
    # Single bash command template
    # Run dbt models, then regular tests, then elementary tests (if enabled)
    bash_command = f"""
                    {flag_init}
                    echo "Running dbt models..."
                    docker exec {docker_container} dbt run {flag_var} --profiles-dir {profiles_dir} --project-dir {project_dir}
                    echo "Running dbt tests..."
                    docker exec {docker_container} dbt test --profiles-dir {profiles_dir} --project-dir {project_dir}
                    {elementary_test_cmd}
                """
    
    return BashOperator(
        task_id=task_id,
        bash_command=bash_command.strip()
    )

