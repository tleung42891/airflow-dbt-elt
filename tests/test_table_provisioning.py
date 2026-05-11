"""Unit tests for utils.table_provisioning."""
from __future__ import annotations

import pytest

from utils import table_provisioning


def test_build_schema_sql_raw_github_contributions():
    sql = table_provisioning._build_schema_sql("raw_github_contributions")
    assert "username" in sql
    assert "VARCHAR(255)" in sql
    assert "contribution_count" in sql
    assert "PRIMARY KEY" in sql


def test_build_schema_sql_raw_github_pulls():
    sql = table_provisioning._build_schema_sql("raw_github_pulls")
    assert "repo_name" in sql
    assert "pr_id" in sql


def test_build_schema_sql_unknown_table_raises():
    with pytest.raises(KeyError):
        table_provisioning._build_schema_sql("nonexistent_table_xyz")


def test_schema_sql_not_null_constraints():
    sql = table_provisioning._build_schema_sql("raw_github_contributions")
    assert "NOT NULL" in sql
    assert "username" in sql


def test_schema_sql_composite_primary_key():
    sql = table_provisioning._build_schema_sql("raw_github_contributions")
    assert "PRIMARY KEY (username, date)" in sql


def test_schema_sql_single_primary_key():
    sql = table_provisioning._build_schema_sql("raw_github_pulls")
    assert "PRIMARY KEY (pr_id)" in sql


def test_create_table_if_not_exists_returns_task_callable():
    task_factory = table_provisioning.create_table_if_not_exists(
        table_name="raw_github_contributions",
        postgres_conn_id="postgres_default",
    )
    assert callable(task_factory)
