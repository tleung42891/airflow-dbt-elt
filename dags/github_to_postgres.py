from __future__ import annotations
import pendulum
import json 
from airflow.decorators import dag, task
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from typing import List, Tuple, Any
from utils.table_provisioning import create_raw_github_pulls_table
from utils.insert_utils import load_data_with_config

GITHUB_CONN_ID = "github_api_conn"
POSTGRES_CONN_ID = "postgres_default" 

MY_PROJECTS = [
    "tleung42891/productivity",
    "tleung42891/develop_health_example",
    "tleung42891/dbt_poc",
    "tleung42891/airflow-dbt-elt",
] 

# --- DAG ---
@dag(
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["github", "multi-project", "demo"],
)
def github_multi_project_pipeline():
    
    @task
    def extract_pull_requests(conn_id: str, repo: str) -> List[Tuple[Any, ...]]:
        """
        Extracts PR data, includes the repo name, and accesses the token correctly.
        """
        
        http_hook = HttpHook(method='GET', http_conn_id=conn_id)
        conn = http_hook.get_connection(conn_id)
        
        # Fixes extra field bug
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
        
        response = http_hook.run(
            endpoint=endpoint, 
            data={"state": "closed", "per_page": 100}, 
            headers=headers
        )
        
        pr_data = response.json()
        
        # 6 fields in order: repo, pr_id, state, created_at, merged_at, user_login
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
    create_table = create_raw_github_pulls_table(postgres_conn_id=POSTGRES_CONN_ID)()
    
    # Loops through all projects
    for project in MY_PROJECTS:
        # Override task_id to ensure unique names for each iteration
        repo_name_safe = project.replace('/', '_')
        raw_pulls = extract_pull_requests.override(task_id=f"extract_{repo_name_safe}")(
            conn_id=GITHUB_CONN_ID, 
            repo=project
        )
        load_task = load_raw_data.override(task_id=f"load_{repo_name_safe}")(
            data=raw_pulls
        )
        
        # Set dependencies
        create_table >> raw_pulls >> load_task

github_multi_project_pipeline()