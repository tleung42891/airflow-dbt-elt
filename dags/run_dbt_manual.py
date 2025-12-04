from __future__ import annotations
import pendulum
from airflow.decorators import dag
from airflow.operators.bash import BashOperator

# --- DAG DEFINITION ---
@dag(
    dag_id="run_dbt_manual",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["dbt", "manual"],
    params={
        "full_refresh": False,
    },
)
def run_dbt_manual():
    """
    Simple DAG to manually run dbt transformations.
    
    Parameters:
    - full_refresh (bool): If True, runs dbt with --full-refresh flag. Default: False
    """
    
    run_dbt_models = BashOperator(
        task_id='run_dbt_transformations',
        bash_command="""
                    # Check if full_refresh parameter is set to true
                    FULL_REFRESH_FLAG=""
                    if [ "{{ params.full_refresh }}" == "True" ] || [ "{{ params.full_refresh }}" == "true" ]; then
                        FULL_REFRESH_FLAG="--full-refresh"
                        echo "Running dbt with --full-refresh flag"
                    else
                        echo "Running dbt in incremental mode (no --full-refresh)"
                    fi
                    
                    OUTPUT=$(docker exec dbt_cli dbt run $FULL_REFRESH_FLAG --profiles-dir /usr/app/dbt --project-dir /usr/app/dbt)
                    echo "$OUTPUT"
                    
                    # Check if the output contains the "success line" and force a "Completed successfully"
                    if echo "$OUTPUT" | grep -q "Completed successfully"; then
                        exit 0 # Force success if the transformation completed
                    else
                        exit 1 # Fail otherwise
                    fi
                """,
    )

run_dbt_manual()

