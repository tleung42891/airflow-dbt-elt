"""
Utility functions for running dbt transformations in Airflow DAGs.
"""
from airflow.operators.bash import BashOperator
from typing import Optional

def create_dbt_run_task(
    task_id: str = 'run_dbt_transformations',
    full_refresh: Optional[bool] = False,
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
        docker_container: Name of the Docker container running dbt. Default: 'dbt_cli'
        profiles_dir: Path to dbt profiles directory in container. Default: '/usr/app/dbt'
        project_dir: Path to dbt project directory in container. Default: '/usr/app/dbt'
    
    Returns:
        BashOperator configured to run dbt transformations
    
    Examples:
        # Direct boolean value
        create_dbt_run_task(full_refresh=True)
        
        # Use Airflow params
        create_dbt_run_task(full_refresh=None)  # Reads from {{ params.full_refresh }}
    """
    # Build the flag initialization logic conditionally
    if full_refresh is None:
        # Use Airflow params template
        flag_init = """
                    # Check if full_refresh parameter is set to true
                    FULL_REFRESH_FLAG=""
                    if [ "{{{{ params.full_refresh }}}}" == "True" ] || [ "{{{{ params.full_refresh }}}}" == "true" ]; then
                        FULL_REFRESH_FLAG="--full-refresh"
                        echo "Running dbt with --full-refresh flag"
                    else
                        echo "Running dbt in incremental mode (no --full-refresh)"
                    fi
                    """
        flag_var = "$FULL_REFRESH_FLAG"
    else:
        # Use direct boolean value
        flag_value = "--full-refresh" if full_refresh else ""
        flag_init = ""
        flag_var = flag_value
    
    # Single bash command template
    bash_command = f"""
                    {flag_init}
                    docker exec {docker_container} dbt run {flag_var} --profiles-dir {profiles_dir} --project-dir {project_dir}
                """
    
    return BashOperator(
        task_id=task_id,
        bash_command=bash_command.strip()
    )

