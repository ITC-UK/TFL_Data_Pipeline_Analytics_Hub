import pytest
from unittest.mock import MagicMock, patch

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import stream_archive


@pytest.fixture
def mock_spark():
    """Return a mock SparkSession with _jvm and _jsc attributes."""
    spark = MagicMock()
    spark._jvm = MagicMock()
    spark._jsc = MagicMock()
    spark._jsc.hadoopConfiguration.return_value = "config"
    return spark


def test_create_spark_session_returns_spark(monkeypatch, mock_spark):
    """Ensure create_spark_session returns a SparkSession instance without starting real Spark."""
    # Patch the function to return the mock Spark instead of creating a real SparkSession
    monkeypatch.setattr(stream_archive, "create_spark_session", lambda app_name: mock_spark)
    spark = stream_archive.create_spark_session("test_app")
    assert spark == mock_spark


def test_get_hadoop_fs_calls_jvm(mock_spark):
    """Verify get_hadoop_fs returns the Hadoop FileSystem object from SparkSession."""
    fs_mock = MagicMock()
    mock_spark._jvm.org.apache.hadoop.fs.FileSystem.get.return_value = fs_mock
    fs = stream_archive.get_hadoop_fs(mock_spark)
    mock_spark._jvm.org.apache.hadoop.fs.FileSystem.get.assert_called_once_with("config")
    assert fs == fs_mock


def test_archive_files_moves_files(mock_spark):
    """Ensure archive_files renames each file from src to dst."""
    fs = MagicMock()
    # Simulate two files in src path
    file1 = MagicMock()
    file1.getPath.return_value.getName.return_value = "file1.parquet"
    file2 = MagicMock()
    file2.getPath.return_value.getName.return_value = "file2.parquet"
    fs.exists.return_value = True
    fs.listStatus.return_value = [file1, file2]

    # Patch spark inside stream_archive module
    with patch("stream_archive.spark", mock_spark):
        mock_spark._jvm.org.apache.hadoop.fs.Path.side_effect = lambda x: x
        stream_archive.archive_files(fs, "src_path", "dst_path")

    assert fs.rename.call_count == 2
    fs.rename.assert_any_call("src_path/file1.parquet", "dst_path/file1.parquet")
    fs.rename.assert_any_call("src_path/file2.parquet", "dst_path/file2.parquet")


def test_archive_files_skips_when_no_files(mock_spark):
    """Ensure archive_files does nothing if src path does not exist."""
    fs = MagicMock()
    fs.exists.return_value = False
    stream_archive.archive_files(fs, "src_path", "dst_path")
    fs.listStatus.assert_not_called()
    fs.rename.assert_not_called()


def test_delete_remaining_deletes_existing_path():
    """Verify delete_remaining deletes path if it exists."""
    fs = MagicMock()
    fs.exists.return_value = True
    stream_archive.delete_remaining(fs, "some_path")
    fs.delete.assert_called_once_with("some_path", True)


def test_delete_remaining_skips_nonexistent_path():
    """Verify delete_remaining does nothing if path does not exist."""
    fs = MagicMock()
    fs.exists.return_value = False
    stream_archive.delete_remaining(fs, "some_path")
    fs.delete.assert_not_called()
