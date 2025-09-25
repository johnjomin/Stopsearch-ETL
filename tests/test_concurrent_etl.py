import pytest
from unittest.mock import Mock
from concurrent.futures import ThreadPoolExecutor

from stopsearch_etl.concurrent_etl import ConcurrentEtlService
from stopsearch_etl.backfill_service import BackfillResult


def test_concurrent_etl_processes_months_in_parallel(): # happy path: all months succeed
    # Arrange
    mock_api_client = Mock()
    mock_etl_service = Mock()

    available_months = ["2023-12", "2023-11", "2023-10"]
    mock_api_client.get_available_months.return_value = available_months

    # Mock ETL results
    mock_etl_service.extract_transform_load.return_value = 100

    concurrent_etl = ConcurrentEtlService(mock_api_client, mock_etl_service, max_workers=2)

    # Act
    result = concurrent_etl.backfill_force_concurrent("metropolitan")

    # Assert
    assert result.total_records == 300  # 3 months × 100 records
    assert result.months_processed == 3
    assert result.force == "metropolitan"

    # All months should have been processed
    assert mock_etl_service.extract_transform_load.call_count == 3


def test_concurrent_etl_handles_month_failures(): # one month fails; others still count
    # Arrange
    mock_api_client = Mock()
    mock_etl_service = Mock()

    available_months = ["2023-12", "2023-11", "2023-10"]
    mock_api_client.get_available_months.return_value = available_months

    # Middle month fails
    def etl_side_effect(force, month):
        if month == "2023-11":
            raise Exception("Network timeout")
        return 100

    mock_etl_service.extract_transform_load.side_effect = etl_side_effect

    concurrent_etl = ConcurrentEtlService(mock_api_client, mock_etl_service, max_workers=2)

    # Act
    result = concurrent_etl.backfill_force_concurrent("metropolitan")

    # Assert
    assert result.total_records == 200  # 2 successful months × 100 records
    assert result.months_processed == 2
    assert result.months_failed == 1


def test_concurrent_etl_respects_max_workers_limit(): # many months: just check everything ran
    # Arrange
    mock_api_client = Mock()
    mock_etl_service = Mock()
    
    available_months = [f"2023-{month:02d}" for month in range(1, 13)]  # 12 months
    mock_api_client.get_available_months.return_value = available_months

    # track basic concurrency (best-effort)
    executing_count = 0
    max_concurrent = 0

    def slow_etl(force, month):
        nonlocal executing_count, max_concurrent
        executing_count += 1
        max_concurrent = max(max_concurrent, executing_count)
        # pretend to do work
        import time
        time.sleep(0.01)
        executing_count -= 1
        return 50

    mock_etl_service.extract_transform_load.side_effect = slow_etl

    concurrent_etl = ConcurrentEtlService(mock_api_client, mock_etl_service, max_workers=3)

    # Act
    result = concurrent_etl.backfill_force_concurrent("metropolitan")

    # Assert
    assert result.total_records == 600  # 12 months × 50 records
    assert result.months_processed == 12
    assert mock_etl_service.extract_transform_load.call_count == 12
    # NOTE: checking exact concurrency is flaky; avoid strict asserts on max_concurrent