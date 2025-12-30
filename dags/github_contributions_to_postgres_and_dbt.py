from __future__ import annotations
import pendulum
import json
import datetime
import requests
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from typing import List, Tuple, Any, Set
from datetime import timedelta
from utils.table_provisioning import create_table_if_not_exists
from utils.insert_utils import load_data_with_config, load_table_config
from utils.dbt_utils import create_dbt_run_task

# --- CONFIGURATION ---
POSTGRES_CONN_ID = "postgres_default"
BASE_API_URL = "https://github-contributions-api.deno.dev/{username}.json"
YEARS_TO_FETCH = [2023, 2024, 2025]
GITHUB_USERNAMES = ["tleung42891", "holmbergf", "TylerAkinsCrisp", "Burkland"]


def find_contribution_dicts(data):
    """Recursively searches for dictionaries containing a 'date' key."""
    contribution_dicts = []
    
    if isinstance(data, dict) and data.get('date'):
        contribution_dicts.append(data)
    
    elif isinstance(data, (list, dict)):
        items = data.values() if isinstance(data, dict) else data
        for item in items:
            contribution_dicts.extend(find_contribution_dicts(item))
            
    return contribution_dicts


# --- DAG DEFINITION ---
@dag(
    dag_id="github_contributions_to_postgres_and_dbt",
    schedule="@daily",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["github", "contributions", "elt"],
)
def github_contributions_to_postgres():
    
    @task
    def get_existing_dates(username: str) -> Set[str]:
        """
        Gets the set of dates that already exist in PostgreSQL for a given username.
        Returns a set of date strings in 'YYYY-MM-DD' format.
        """
        postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = postgres_hook.get_conn()
        cursor = conn.cursor()
        
        try:
            query = """
                SELECT DISTINCT date::text 
                FROM raw_github_contributions 
                WHERE username = %s
                ORDER BY date
            """
            cursor.execute(query, (username,))
            existing_dates = {row[0] for row in cursor.fetchall()}
            print(f"Found {len(existing_dates)} existing dates for {username}")
            return existing_dates
        except Exception as e:
            # If table doesn't exist yet, return empty set
            print(f"No existing data found for {username} (this is normal for first run): {e}")
            return set()
        finally:
            cursor.close()
            conn.close()
    
    @task
    def extract_contributions(username: str, existing_dates: Set[str]) -> List[Tuple[Any, ...]]:
        """
        Extracts GitHub contribution data incrementally for a single username.
        Only fetches dates that don't already exist in the database.
        For scheduled daily runs, focuses on recent dates. For manual runs, can do full backfill.
        Returns a list of tuples: (username, date, contribution_count)
        """
        print(f"\n--- Fetching data for USER: {username} ---")
        
        # If we have very few existing dates (< backfill_threshold), do a full backfill. 
        # Otherwise, do incremental (last N days based on restatement_window or default to 30 days if not specified)) to catch new/missed days
        today = datetime.datetime.now().date()
        
        table_config = load_table_config("raw_github_contributions")
        backfill_threshold = table_config.get("backfill_threshold", 30)
        
        if len(existing_dates) < backfill_threshold:
            # Full backfill: fetch all configured years
            date_ranges = []
            for year in YEARS_TO_FETCH:
                year_start = f"{year}-01-01"
                year_end = f"{year}-12-31"
                if year == today.year:
                    year_end = today.strftime("%Y-%m-%d")
                date_ranges.append((year_start, year_end))
            print(f"  📅 Full backfill mode: {len(existing_dates)} existing dates, fetching all configured years")
        else:
            # Incremental mode: fetch last N days (based on restatement_window or default to 7 days if not specified) to catch new days
            restatement_window = table_config.get("restatement_window", 7)
            start_date = today - timedelta(days=restatement_window)
            end_date = today
            date_ranges = [(start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"))]
            print(f"  📅 Incremental mode: {len(existing_dates)} existing dates, fetching last {restatement_window} days ({start_date} to {end_date})")
        
        all_daily_contributions = {}
        api_base = BASE_API_URL.format(username=username)
        new_dates_count = 0
        skipped_dates_count = 0
        
        for start_date, end_date in date_ranges:
            # Construct the API URL with the date range
            api_url_with_dates = f"{api_base}?from={start_date}&to={end_date}"
            print(f"  -> Fetching data from {start_date} to {end_date}...")
            
            data = None
            
            try:
                response = requests.get(api_url_with_dates)
                response.raise_for_status() 
                
                # JSON cleaning logic
                raw_text = response.text.strip()
                start_index = raw_text.find('{')
                
                if start_index == -1:
                    print(f"  ❌ Date range {start_date} to {end_date}: Response did not contain a valid JSON object. Skipping.")
                    continue
                
                clean_text = raw_text[start_index:]
                data = json.loads(clean_text)
                
            except requests.exceptions.RequestException as e:
                print(f"  ❌ Date range {start_date} to {end_date}: Failed to fetch data: {e}. Skipping.")
                continue
            except json.JSONDecodeError as e:
                print(f"  ❌ Date range {start_date} to {end_date}: Failed to decode JSON: {e}. Skipping.")
                continue
            
            # --- Data Processing (Recursive Search) ---
            if data is not None:
                all_contributions = find_contribution_dicts(data)
                
                for item in all_contributions:
                    date = item.get('date')
                    count = item.get('contributionCount') 
                    
                    if date and count is not None:
                        # Only include dates that don't already exist (incremental logic)
                        if date not in existing_dates:
                            try:
                                all_daily_contributions[date] = int(count)
                                new_dates_count += 1
                            except ValueError:
                                pass
                        else:
                            skipped_dates_count += 1

        print(f"  ✅ Found {new_dates_count} new dates to load (skipped {skipped_dates_count} existing dates)")

        # Convert to list of tuples for PostgreSQL insertion
        # Format: (username, date, contribution_count)
        records = []
        for date, count in all_daily_contributions.items():
            records.append((username, date, count))
        
        return records

    @task
    def load_raw_data(data: List[Tuple[Any, ...]]):
        """Loads the extracted records into the raw PostgreSQL table using YAML-configured upsert logic."""
        postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        load_data_with_config(
            postgres_hook=postgres_hook,
            table_name="raw_github_contributions",
            data=data
        )

    # Create table first
    create_table_task = create_table_if_not_exists(
        table_name="raw_github_contributions",
        postgres_conn_id=POSTGRES_CONN_ID
    )()
    
    # Extract and Load in Parallel for all usernames
    all_load_tasks = []

    for username in GITHUB_USERNAMES:
        username_safe = username.replace('-', '_').replace('.', '_')
        
        # Get existing dates for this username
        existing_dates = get_existing_dates.override(task_id=f"get_existing_dates_{username_safe}")(
            username=username
        )
        
        # Extract (only missing dates)
        contributions = extract_contributions.override(task_id=f"extract_{username_safe}")(
            username=username,
            existing_dates=existing_dates
        )
        
        # Load
        load_task = load_raw_data.override(task_id=f"load_{username_safe}")(
            data=contributions
        )
        all_load_tasks.append(load_task)
        
        # Set dependencies: create_table_if_not_exists -> get_existing_dates -> extract -> load
        create_table_task >> existing_dates >> contributions >> load_task

    # Transformation
    run_dbt_models = create_dbt_run_task()

    # The dbt transformation waits for all parallel load tasks to complete successfully.
    all_load_tasks >> run_dbt_models

github_contributions_to_postgres()

