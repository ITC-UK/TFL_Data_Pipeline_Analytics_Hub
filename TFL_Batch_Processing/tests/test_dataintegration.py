import os
import pandas as pd
import pytest
from unittest.mock import MagicMock, patch
import sys

# Ensure correct 'src' folder
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../TFL_Batch Processing/src')))

from bronze.dataintegration import load_csv_to_postgres, get_engine
from pandas.errors import ParserError


# ---------- Fixtures ----------

@pytest.fixture
def mock_listdir():
    with patch("os.listdir") as mock:
        yield mock

@pytest.fixture
def mock_read_csv():
    with patch("pandas.read_csv") as mock:
        yield mock

@pytest.fixture
def mock_engine():
    with patch("bronze.dataintegration.get_engine") as mock:
        # Return a MagicMock engine
        mock.return_value = MagicMock()
        yield mock


# ---------- Tests ----------

def test_load_csv_to_postgres_success(mock_listdir, mock_read_csv, mock_engine):
    """Test successful CSV loading into Postgres."""
    mock_listdir.return_value = ["tfl_line1.csv", "tfl_line2.csv"]
    mock_df = MagicMock()
    mock_read_csv.return_value = mock_df

    load_csv_to_postgres()

    assert mock_read_csv.call_count == 2
    assert mock_df.to_sql.call_count == 2
    expected_tables = ["TFL_tfl_line1_lines", "TFL_tfl_line2_lines"]
    actual_tables = [c.args[0] for c in mock_df.to_sql.call_args_list]
    assert actual_tables == expected_tables


def test_load_csv_to_postgres_insert_error(mock_listdir, mock_read_csv, mock_engine):
    """Test error handling during insert."""
    mock_listdir.return_value = ["tfl_line1.csv"]
    mock_df = MagicMock()
    mock_df.to_sql.side_effect = Exception("DB error")
    mock_read_csv.return_value = mock_df

    with patch("builtins.print") as mock_print:
        load_csv_to_postgres()
        printed = [c.args[0] for c in mock_print.call_args_list]
        assert any("Error inserting" in text for text in printed)
        assert any("DB error" in text for text in printed)


def test_no_csv_files(mock_engine, mock_listdir, mock_read_csv):
    """Ensure function runs without CSV files (no exception raised)."""
    mock_listdir.return_value = []  # No CSV files
    load_csv_to_postgres()          # Should complete without exception


def test_partial_failure(mock_engine):
    """Test scenario where one CSV succeeds and another fails."""
    files = ["good.csv", "bad.csv"]
    good_df = MagicMock()
    bad_df = MagicMock()
    bad_df.to_sql.side_effect = Exception("DB error")
    df_map = {"good.csv": good_df, "bad.csv": bad_df}

    def read_csv_side_effect(file_path):
        filename = os.path.basename(file_path)
        return df_map[filename]

    with patch("os.listdir", return_value=files), \
         patch("pandas.read_csv", side_effect=read_csv_side_effect), \
         patch("builtins.print") as mock_print:
        load_csv_to_postgres()
        printed = [c.args[0] for c in mock_print.call_args_list]
        assert any("DB error" in text for text in printed)
        assert any("rows appended" in text for text in printed)


def test_empty_csv_file(mock_engine):
    """Test behavior when a CSV is empty."""
    with patch("os.listdir", return_value=["empty.csv"]), \
         patch("pandas.read_csv", return_value=pd.DataFrame()), \
         patch("builtins.print") as mock_print:
        load_csv_to_postgres()
        printed = [c.args[0] for c in mock_print.call_args_list]
        assert any("0 rows appended" in text for text in printed)


def test_invalid_csv_format(mock_engine, mock_listdir):
    """Test handling of invalid CSV format (ParserError)."""
    mock_listdir.return_value = ["bad.csv"]
    with patch("pandas.read_csv", side_effect=ParserError("bad format")), \
         patch("builtins.print") as mock_print:
        load_csv_to_postgres()
        printed = [c.args[0] for c in mock_print.call_args_list]
        assert any("Error reading" in text for text in printed)
        assert any("bad format" in text for text in printed)


def test_db_connection_failure():
    """Test DB connection failure handling."""
    with patch("bronze.dataintegration.create_engine", side_effect=Exception("DB connection failed")), \
         patch("builtins.print") as mock_print:
        try:
            get_engine()
        except Exception:
            pass
        printed = [c.args[0] for c in mock_print.call_args_list]
        assert any("DB connection failed" in text for text in printed)
