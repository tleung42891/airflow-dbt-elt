from __future__ import annotations
import pendulum
import json 
from airflow.decorators import dag, task
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from typing import List, Tuple, Any
from utils.table_provisioning import create_table_if_not_exists
from utils.insert_utils import load_data_with_config
from utils.dbt_utils import create_dbt_run_task

# --- CONFIGURATION ---
GITHUB_CONN_ID = "github_api_conn"
POSTGRES_CONN_ID = "postgres_default" 

MY_PROJECTS = [
    "tleung42891/productivity",
    "tleung42891/develop_health_example",
    "tleung42891/dbt_poc",
    "tleung42891/airflow-dbt-elt",
] 

# --- DAG DEFINITION ---
@dag(
    dag_id="github_to_postgres_and_dbt", 
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["github", "dbt", "elt"],
)
def github_to_postgres_and_dbt():
    
    @task
    def extract_pull_requests(conn_id: str, repo: str) -> List[Tuple[Any, ...]]:
        """
        Extracts closed PR data for a specific repository using the GitHub API token.
        """
        http_hook = HttpHook(method='GET', http_conn_id=conn_id)
        conn = http_hook.get_connection(conn_id)
        
        # Retrieve Token from 'Extra' field
        try:
            extra_conf = json.loads(conn.extra)
            token = extra_conf['token']
        except (json.JSONDecodeError, KeyError) as e:
            raise ValueError(f"Error accessing GitHub token from connection 'Extra' field: {e}")

        endpoint = f"/repos/{repo}/pulls"
        headers = {
            "Authorization": f"token {token}",
            "Accept": "application/vnd.github.v3+json"
        }
        
        # Make the API Call (state=closed, 100 per page)
        response = http_hook.run(
            endpoint=endpoint, 
            data={"state": "closed", "per_page": 100}, 
            headers=headers
        )
        
        pr_data = response.json()
        
        # Format Data for Postgres Insertion (repo_name, pr_id, state, created_at, merged_at, user_login)
        records = []
        for pr in pr_data:
            records.append((
                repo,
                pr.get('id'),
                pr.get('state'),
                pr.get('created_at'),
                pr.get('merged_at'),
                pr.get('user', {}).get('login')
            ))
            
        print(f"Prepared {len(records)} pull request records for {repo}.")
        return records

    @task
    def load_raw_data(data: List[Tuple[Any, ...]]):
        """Loads the extracted records into the raw PostgreSQL table using YAML-configured upsert logic."""
        postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        load_data_with_config(
            postgres_hook=postgres_hook,
            table_name="raw_github_pulls",
            data=data
        )

    # Always try to create table first
    create_table_task = create_table_if_not_exists(
        table_name="raw_github_pulls",
        postgres_conn_id=POSTGRES_CONN_ID
    )()
    
    #  Extract and Load in Parallel
    all_load_tasks = []

    for project in MY_PROJECTS:
        repo_name_safe = project.replace('/', '_')
        
        # Extract
        raw_pulls = extract_pull_requests.override(task_id=f"extract_{repo_name_safe}")(
            conn_id=GITHUB_CONN_ID, 
            repo=project
        )
        
        # Load
        load_task = load_raw_data.override(task_id=f"load_{repo_name_safe}")(
            data=raw_pulls
        )
        all_load_tasks.append(load_task)
        
        # Set dependency: create_table_if_not_exists -> extract -> load
        create_table_task >> raw_pulls >> load_task

    # Transformation
    run_dbt_models = create_dbt_run_task()

    # The dbt transformation waits for all parallel load tasks to complete successfully.
    all_load_tasks >> run_dbt_models

github_to_postgres_and_dbt()