"""Unit tests for utils.insert_utils."""
from __future__ import annotations

import pytest

from utils.insert_utils import load_data_with_config, load_table_config


def test_load_table_config_raw_github_contributions():
    cfg = load_table_config("raw_github_contributions")
    assert cfg["method"] == "executemany"
    assert "schema" in cfg
    assert "username" in cfg["schema"]["columns"]
    assert cfg.get("restatement_window") == 7


def test_load_table_config_raw_github_pulls():
    cfg = load_table_config("raw_github_pulls")
    assert "schema" in cfg
    assert "repo_name" in cfg["schema"]["columns"]


def test_load_table_config_missing_table_raises():
    with pytest.raises(KeyError, match="not found"):
        load_table_config("nonexistent_table_xyz")


def test_load_table_config_raw_github_pulls_method():
    cfg = load_table_config("raw_github_pulls")
    assert cfg["method"] == "insert_rows"
    assert cfg["upsert"]["type"] == "replace_index"
    assert cfg["upsert"]["conflict_key"] == "pr_id"


def test_load_table_config_columns_derivable_from_schema():
    for table in ("raw_github_contributions", "raw_github_pulls"):
        cfg = load_table_config(table)
        columns = list(cfg["schema"]["columns"].keys())
        assert len(columns) >= 2, f"{table} should have at least 2 columns"


def test_load_data_with_config_noop_empty_data(mocker):
    mock_hook = mocker.MagicMock()
    load_data_with_config(postgres_hook=mock_hook, table_name="raw_github_contributions", data=[])
    mock_hook.get_conn.assert_not_called()


def test_load_data_with_config_raises_without_hook_or_conn_id():
    with pytest.raises(ValueError, match="Either postgres_hook or postgres_conn_id"):
        load_data_with_config(postgres_hook=None, table_name="raw_github_contributions", data=[(1,)])
