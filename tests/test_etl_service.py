import pytest
from unittest.mock import Mock, call
from datetime import datetime

from stopsearch_etl.domain import StopSearchRecord
from stopsearch_etl.etl_service import EtlService


def test_etl_service_processes_single_force_month():
    # Arrange
    mock_api_client = Mock()
    mock_repository = Mock()

    # Mock API response
    api_response = [
        {
            "type": "Person search",
            "datetime": "2023-01-15T14:30:00+00:00",
            "gender": "Male",
            "age_range": "25-34",
            "self_defined_ethnicity": "White - English",
            "officer_defined_ethnicity": "White",
            "legislation": "Police and Criminal Evidence Act 1984 (section 1)",
            "object_of_search": "Drugs",
            "outcome": "A no further action disposal",
            "outcome_linked_to_object_of_search": False,
            "removal_of_more_than_outer_clothing": False,
            "location": {
                "latitude": 51.5074,
                "longitude": -0.1278,
                "street": {
                    "id": 883407,
                    "name": "On or near High Street"
                }
            }
        }
    ]

    mock_api_client.fetch_stops.return_value = api_response
    mock_repository.save_batch.return_value = 1

    etl_service = EtlService(mock_api_client, mock_repository)

    # Act
    result = etl_service.extract_transform_load("metropolitan", "2023-01")

    # Assert
    assert result == 1
    mock_api_client.fetch_stops.assert_called_once_with("metropolitan", "2023-01")

    # Verify save_batch was called with the right domain object
    mock_repository.save_batch.assert_called_once()
    saved_records = mock_repository.save_batch.call_args[0][0]
    assert len(saved_records) == 1
    assert isinstance(saved_records[0], StopSearchRecord)
    assert saved_records[0].type == "Person search"
    assert saved_records[0].gender == "Male"


def test_etl_service_handles_empty_api_response():
    # Arrange
    mock_api_client = Mock()
    mock_repository = Mock()

    mock_api_client.fetch_stops.return_value = []
    mock_repository.save_batch.return_value = 0

    etl_service = EtlService(mock_api_client, mock_repository)

    # Act
    result = etl_service.extract_transform_load("metropolitan", "2023-01")

    # Assert
    assert result == 0
    mock_repository.save_batch.assert_called_once_with([])


def test_etl_service_handles_malformed_records():
    # Arrange
    mock_api_client = Mock()
    mock_repository = Mock()

    # missing optional fields is fine; from_api_data should cope
    api_response = [
        {
            "type": "Vehicle search",
            "datetime": "2023-02-01T00:00:00Z",
            # Missing many optional fields
        }
    ]

    mock_api_client.fetch_stops.return_value = api_response
    mock_repository.save_batch.return_value = 1

    etl_service = EtlService(mock_api_client, mock_repository)

    # Act
    result = etl_service.extract_transform_load("avon-and-somerset", "2023-02")

    # Assert
    assert result == 1
    saved_records = mock_repository.save_batch.call_args[0][0]
    assert len(saved_records) == 1
    assert saved_records[0].type == "Vehicle search"
    assert saved_records[0].gender is None  # Should handle missing fields gracefully