"""
Shared table provisioning utilities for Airflow DAGs.

This module provides reusable functions to create database tables
if they don't exist, ensuring consistent schema definitions across DAGs.
"""
import yaml
from pathlib import Path
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook


def _load_table_config(table_name: str) -> dict:
    """
    Loads the table configuration from YAML.
    
    Args:
        table_name: Name of the table to get configuration for
        
    Returns:
        Dictionary containing table configuration
        
    Raises:
        FileNotFoundError: If config file doesn't exist
        KeyError: If table_name not found in config
    """
    dags_dir = Path(__file__).parent.parent
    config_path = dags_dir / "table_configs" / "table_insert_configs.yaml"
    
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")
    
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    if table_name not in config.get('tables', {}):
        available_tables = list(config.get('tables', {}).keys())
        raise KeyError(
            f"Table '{table_name}' not found in config. "
            f"Available tables: {available_tables}"
        )
    
    return config['tables'][table_name]


def _build_schema_sql(table_name: str) -> str:
    """
    Builds SQL schema string from YAML configuration.
    
    Args:
        table_name: Name of the table
        
    Returns:
        SQL schema string (column definitions and constraints)
    """
    config = _load_table_config(table_name)
    
    if 'schema' not in config:
        raise ValueError(
            f"Table '{table_name}' does not have a 'schema' definition in the config file."
        )
    
    schema_config = config['schema']
    columns = schema_config.get('columns', {})
    primary_key = schema_config.get('primary_key', [])
    
    # Build column definitions
    column_defs = []
    for col_name, col_info in columns.items():
        col_type = col_info['type']
        not_null = col_info.get('not_null', False)
        not_null_str = " NOT NULL" if not_null else ""
        column_defs.append(f"        {col_name} {col_type}{not_null_str}")
    
    # Add primary key constraint if specified
    schema_sql = ",\n".join(column_defs)
    if primary_key:
        if isinstance(primary_key, list):
            pk_cols = ", ".join(primary_key)
        else:
            pk_cols = primary_key
        schema_sql += f",\n        PRIMARY KEY ({pk_cols})"
    
    return schema_sql


def create_table_if_not_exists(
    table_name: str,
    table_schema: str = None,
    postgres_conn_id: str = "postgres_default"
):
    """
    Creates a PostgreSQL table if it doesn't exist.
    
    If table_schema is not provided, it will be built from the YAML configuration
    file based on table_name.
    
    Args:
        table_name: Name of the table to create
        table_schema: Optional SQL column definitions and constraints.
                     If not provided, will be built from YAML config for table_name.
        postgres_conn_id: PostgreSQL connection ID (default: "postgres_default")
    
    Returns:
        Airflow task function that creates the table
    
    Raises:
        ValueError: If table_schema is not provided and table_name is not in config
        FileNotFoundError: If config file doesn't exist
    """
    # Build schema from YAML config if not provided
    if table_schema is None:
        table_schema = _build_schema_sql(table_name)
    
    @task
    def create_table_if_not_exists():
        """Creates the specified table if it doesn't exist."""
        postgres_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        conn = postgres_hook.get_conn()
        cursor = conn.cursor()
        
        try:
            create_table_sql = f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    {table_schema}
                );
            """
            cursor.execute(create_table_sql)
            conn.commit()
            print(f"Table {table_name} created or already exists.")
        except Exception as e:
            conn.rollback()
            raise Exception(f"Error creating table {table_name}: {e}")
        finally:
            cursor.close()
            conn.close()
    
    return create_table_if_not_exists

