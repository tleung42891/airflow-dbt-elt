"""
Shared table provisioning utilities for Airflow DAGs.

This module provides reusable functions to create database tables
if they don't exist, ensuring consistent schema definitions across DAGs.
"""
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook


def create_raw_github_pulls_table(postgres_conn_id: str = "postgres_default"):
    """
    Creates the raw_github_pulls table if it doesn't exist.
    
    Schema:
    - repo_name: VARCHAR(255) NOT NULL
    - pr_id: BIGINT NOT NULL (PRIMARY KEY)
    - state: VARCHAR(50)
    - created_at: TIMESTAMP
    - merged_at: TIMESTAMP
    - user_login: VARCHAR(255)
    
    Args:
        postgres_conn_id: PostgreSQL connection ID (default: "postgres_default")
    
    Returns:
        Airflow task function that creates the table
    """
    @task
    def create_table_if_not_exists():
        """Creates the raw_github_pulls table if it doesn't exist."""
        postgres_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        conn = postgres_hook.get_conn()
        cursor = conn.cursor()
        
        try:
            create_table_sql = """
                CREATE TABLE IF NOT EXISTS raw_github_pulls (
                    repo_name VARCHAR(255) NOT NULL,
                    pr_id BIGINT NOT NULL,
                    state VARCHAR(50),
                    created_at TIMESTAMP,
                    merged_at TIMESTAMP,
                    user_login VARCHAR(255),
                    PRIMARY KEY (pr_id)
                );
            """
            cursor.execute(create_table_sql)
            conn.commit()
            print("Table raw_github_pulls created or already exists.")
        except Exception as e:
            conn.rollback()
            raise Exception(f"Error creating table raw_github_pulls: {e}")
        finally:
            cursor.close()
            conn.close()
    
    return create_table_if_not_exists


def create_raw_github_contributions_table(postgres_conn_id: str = "postgres_default"):
    """
    Creates the raw_github_contributions table if it doesn't exist.
    
    Schema:
    - username: VARCHAR(255) NOT NULL
    - date: DATE NOT NULL
    - contribution_count: INTEGER NOT NULL
    - PRIMARY KEY (username, date)
    
    Args:
        postgres_conn_id: PostgreSQL connection ID (default: "postgres_default")
    
    Returns:
        Airflow task function that creates the table
    """
    @task
    def create_table_if_not_exists():
        """Creates the raw_github_contributions table if it doesn't exist."""
        postgres_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        conn = postgres_hook.get_conn()
        cursor = conn.cursor()
        
        try:
            create_table_sql = """
                CREATE TABLE IF NOT EXISTS raw_github_contributions (
                    username VARCHAR(255) NOT NULL,
                    date DATE NOT NULL,
                    contribution_count INTEGER NOT NULL,
                    PRIMARY KEY (username, date)
                );
            """
            cursor.execute(create_table_sql)
            conn.commit()
            print("Table raw_github_contributions created or already exists.")
        except Exception as e:
            conn.rollback()
            raise Exception(f"Error creating table raw_github_contributions: {e}")
        finally:
            cursor.close()
            conn.close()
    
    return create_table_if_not_exists

