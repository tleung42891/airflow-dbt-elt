from __future__ import annotations
import pendulum
from airflow.decorators import dag
from utils.dbt_utils import create_dbt_run_task

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
    
    run_dbt_models = create_dbt_run_task(full_refresh=None)

run_dbt_manual()

