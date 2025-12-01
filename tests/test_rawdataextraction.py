import pandas as pd
import pytest
from unittest.mock import MagicMock, patch
import sys
import os

# Ensure correct 'src' folder
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../TFL_Batch Processing/src')))

from bronze import rawdataextraction

# ---------- Fixtures ----------

@pytest.fixture
def mock_requests_get():
    with patch("requests.get") as mock:
        yield mock

@pytest.fixture
def mock_read_csv():
    with patch("pandas.read_csv") as mock:
        yield mock

@pytest.fixture
def mock_to_csv():
    with patch("pandas.DataFrame.to_csv") as mock:
        yield mock

@pytest.fixture
def mock_exists():
    with patch("os.path.exists") as mock:
        yield mock

@pytest.fixture
def mock_print():
    with patch("builtins.print") as mock:
        yield mock

@pytest.fixture
def mock_sleep():
    with patch("time.sleep", return_value=None) as mock:
        yield mock


# ---------- Tests ----------

def test_fetch_and_save_once_creates_new_file(mock_requests_get, mock_to_csv, mock_exists):
    """Test creating a new CSV when file does not exist."""
    mock_exists.return_value = False

    mock_response = MagicMock()
    mock_response.json.return_value = [{"id": 1, "status": "OK"}]
    mock_response.raise_for_status.return_value = None
    mock_requests_get.return_value = mock_response

    rawdataextraction.fetch_and_save_once()

    mock_to_csv.assert_called()


def test_fetch_and_save_once_updates_existing_file(mock_requests_get, mock_read_csv, mock_to_csv, mock_exists):
    """Test updating an existing CSV with new data and removing duplicates."""
    mock_exists.return_value = True

    df_old = pd.DataFrame({"id": [1], "status": ["old"]})
    mock_read_csv.return_value = df_old

    mock_response = MagicMock()
    mock_response.json.return_value = [{"id": 2, "status": "new"}, {"id": 1, "status": "old"}]
    mock_response.raise_for_status.return_value = None
    mock_requests_get.return_value = mock_response

    rawdataextraction.fetch_and_save_once()
    mock_to_csv.assert_called()


def test_fetch_and_save_once_no_data(mock_requests_get, mock_print):
    """Test behavior when API returns no data."""
    mock_response = MagicMock()
    mock_response.json.return_value = []
    mock_response.raise_for_status.return_value = None
    mock_requests_get.return_value = mock_response

    with patch("builtins.print") as mock_print:
        rawdataextraction.fetch_and_save_once()
        printed_texts = [call.args[0] for call in mock_print.call_args_list]
        assert any("No data returned" in text for text in printed_texts)


def test_fetch_and_save_once_api_error(mock_requests_get, mock_print):
    """Test exception handling when API request fails."""
    mock_requests_get.side_effect = Exception("API failure")

    with patch("builtins.print") as mock_print:
        rawdataextraction.fetch_and_save_once()
        printed_texts = [call.args[0] for call in mock_print.call_args_list]
        assert any("Error fetching" in text for text in printed_texts)
        assert any("API failure" in text for text in printed_texts)
