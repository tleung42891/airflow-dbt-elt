"""
Utility functions for loading data into PostgreSQL tables using YAML configuration.

This module provides a clean, configuration-driven approach to handling INSERT/UPSERT
operations, eliminating hardcoded SQL statements in DAG files.
"""
import yaml
from pathlib import Path
from typing import List, Tuple, Any, Dict, Optional
from airflow.providers.postgres.hooks.postgres import PostgresHook


def load_table_config(table_name: str) -> Dict:
    """
    Loads the insert configuration for a specific table from YAML.
    
    Args:
        table_name: Name of the table to get configuration for
        
    Returns:
        Dictionary containing table configuration (columns, upsert settings, method)
        
    Raises:
        FileNotFoundError: If config file doesn't exist
        KeyError: If table_name not found in config
    """
    # Get the config file path (table_configs is inside dags/ which is mounted in Docker)
    dags_dir = Path(__file__).parent.parent
    config_path = dags_dir / "table_configs" / "table_insert_configs.yaml"
    
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")
    
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    if table_name not in config.get('tables', {}):
        raise KeyError(f"Table '{table_name}' not found in configuration")
    
    return config['tables'][table_name]


def load_data_with_config(
    postgres_hook: PostgresHook,
    table_name: str,
    data: List[Tuple[Any, ...]],
    postgres_conn_id: Optional[str] = None
) -> None:
    """
    Loads data into a PostgreSQL table using configuration from YAML.
    
    Supports two methods:
    1. executemany: Raw SQL with ON CONFLICT (for composite keys)
    2. insert_rows: PostgresHook.insert_rows() with replace_index (for single key)
    
    Args:
        postgres_hook: PostgresHook instance (or None if postgres_conn_id provided)
        table_name: Name of the target table
        data: List of tuples containing data to insert
        postgres_conn_id: PostgreSQL connection ID (if postgres_hook not provided)
        
    Raises:
        ValueError: If invalid configuration or method
        Exception: If database operation fails
    """
    if not data:
        print(f"No data to load into {table_name}.")
        return
    
    # Get hook if not provided
    if postgres_hook is None:
        if postgres_conn_id is None:
            raise ValueError("Either postgres_hook or postgres_conn_id must be provided")
        postgres_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    
    # Load configuration
    config = load_table_config(table_name)
    
    # Derive columns from schema if not explicitly defined
    if 'columns' in config:
        columns = config['columns']
    elif 'schema' in config and 'columns' in config['schema']:
        # Derive column list from schema definition
        columns = list(config['schema']['columns'].keys())
    else:
        raise ValueError(
            f"Table '{table_name}' must have either 'columns' list or 'schema.columns' definition"
        )
    
    method = config.get('method', 'executemany')
    upsert_config = config.get('upsert', {})
    
    if method == 'executemany':
        # Use raw SQL with ON CONFLICT
        if upsert_config.get('type') != 'on_conflict':
            raise ValueError(f"Table {table_name} uses executemany but upsert.type is not 'on_conflict'")
        
        conflict_keys = upsert_config['conflict_keys']
        update_fields = upsert_config.get('update_fields', {})
        
        # Build ON CONFLICT clause
        conflict_clause = f"({', '.join(conflict_keys)})"
        update_clauses = []
        for field, value in update_fields.items():
            update_clauses.append(f"{field} = {value}")
        
        update_clause = ", ".join(update_clauses) if update_clauses else ""
        
        # Build SQL statement
        columns_str = ", ".join(columns)
        placeholders = ", ".join(["%s"] * len(columns))
        
        upsert_sql = f"""
            INSERT INTO {table_name} ({columns_str})
            VALUES ({placeholders})
            ON CONFLICT {conflict_clause}
            DO UPDATE SET {update_clause}
        """
        
        # Execute
        conn = postgres_hook.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.executemany(upsert_sql, data)
            conn.commit()
            print(f"Successfully loaded {len(data)} records into {table_name} using executemany.")
        except Exception as e:
            conn.rollback()
            raise Exception(f"Error loading data into {table_name}: {e}")
        finally:
            cursor.close()
            conn.close()
    
    elif method == 'insert_rows':
        # Use PostgresHook.insert_rows() with replace_index
        if upsert_config.get('type') != 'replace_index':
            raise ValueError(f"Table {table_name} uses insert_rows but upsert.type is not 'replace_index'")
        
        conflict_key = upsert_config['conflict_key']
        
        postgres_hook.insert_rows(
            table=table_name,
            rows=data,
            target_fields=columns,
            replace=True,
            replace_index=conflict_key
        )
        
        print(f"Successfully loaded {len(data)} records into {table_name} using insert_rows.")
    
    else:
        raise ValueError(f"Unknown method '{method}' for table {table_name}. Must be 'executemany' or 'insert_rows'")

