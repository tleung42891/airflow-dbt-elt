from __future__ import annotations
import pendulum
from airflow.decorators import dag
from utils.dbt_utils import create_dbt_run_task

# --- DAG DEFINITION ---
@dag(
    dag_id="run_dbt",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["dbt"],
    params={
        "full_refresh": False,
        "elementary": False,
        "drop_stale_relations": False,
        "select": "",
    },
)
def run_dbt():
    """
    Runs dbt transformations. Triggered by the GitHub ingestion DAGs and can also be run manually.

    Parameters:
    - full_refresh (bool): If True, runs dbt with --full-refresh flag. Default: False
    - elementary (bool): If True, runs elementary tests after dbt run. Default: False
    - drop_stale_relations (bool): If True, drops stale relations after dbt tests. Default: False
    - select (str): dbt selector applied to run/test (e.g. 'tag:pulls'). Empty = all models. Default: ""
    """

    run_dbt_models = create_dbt_run_task(full_refresh=None, elementary=None, drop_stale_relations=None, select=None)

run_dbt()
