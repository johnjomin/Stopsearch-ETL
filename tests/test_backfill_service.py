import pytest
from unittest.mock import Mock

from stopsearch_etl.backfill_service import BackfillService


def test_backfill_service_processes_all_available_months(): # happy path: 3 months, all succeed
    # Arrange
    mock_api_client = Mock()
    mock_etl_service = Mock()

    # Mock availability discovery
    available_months = ["2023-12", "2023-11", "2023-10"]
    mock_api_client.get_available_months.return_value = available_months

    # Mock ETL results
    mock_etl_service.extract_transform_load.side_effect = [150, 200, 175]

    backfill_service = BackfillService(mock_api_client, mock_etl_service)

    # Act
    result = backfill_service.backfill_force("metropolitan")

    # Assert
    assert result.total_records == 525  # 150 + 200 + 175
    assert result.months_processed == 3
    assert result.force == "metropolitan"

    # Verify ETL was called for each month
    expected_calls = [
        (("metropolitan", "2023-12"),),
        (("metropolitan", "2023-11"),),
        (("metropolitan", "2023-10"),)
    ]
    assert mock_etl_service.extract_transform_load.call_args_list == expected_calls


def test_backfill_service_handles_no_available_months(): # no months -> quick exit
    # Arrange
    mock_api_client = Mock()
    mock_etl_service = Mock()

    mock_api_client.get_available_months.return_value = []

    backfill_service = BackfillService(mock_api_client, mock_etl_service)

    # Act
    result = backfill_service.backfill_force("nonexistent-force")

    # Assert
    assert result.total_records == 0
    assert result.months_processed == 0
    assert result.force == "nonexistent-force"

    # ETL should not have been called
    mock_etl_service.extract_transform_load.assert_not_called()


def test_backfill_service_continues_on_single_month_failure(): # middle month fails, others still processed
    # Arrange
    mock_api_client = Mock()
    mock_etl_service = Mock()

    available_months = ["2023-12", "2023-11", "2023-10"]
    mock_api_client.get_available_months.return_value = available_months

    # Middle month fails, others succeed
    def etl_side_effect(force, month):
        if month == "2023-11":
            raise Exception("API error")
        return 100

    mock_etl_service.extract_transform_load.side_effect = etl_side_effect

    backfill_service = BackfillService(mock_api_client, mock_etl_service)

    # Act
    result = backfill_service.backfill_force("metropolitan")

    # Assert
    assert result.total_records == 200  # 100 + 0 (failed) + 100
    assert result.months_processed == 2  # Only successful months
    assert result.months_failed == 1
    assert mock_etl_service.extract_transform_load.call_count == 3 # all months should have been attempted