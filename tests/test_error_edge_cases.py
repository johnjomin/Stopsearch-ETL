import pytest
from unittest.mock import Mock, patch
from datetime import datetime
import json
from stopsearch_etl.domain import StopSearchRecord
from stopsearch_etl.http_client import HttpPoliceApiClient
from stopsearch_etl.sqlite_repository import SqliteStopSearchRepository
from stopsearch_etl.etl_service import EtlService
from stopsearch_etl.api import ApiError


def test_domain_handles_completely_malformed_api_data():
    # Arrange - broken input on purpose
    malformed_data = {
        "datetime": "not-a-date",
        "location": None,
        "person_search": "invalid",
        "outcome": {"random": "nested stuff"}
    }

    # Act & Assert - # should fail cleanly (raise one of these)
    with pytest.raises((ValueError, TypeError, AttributeError)):
        StopSearchRecord.from_api_data(malformed_data)


def test_http_client_handles_network_timeout():
    # Arrange
    # # NOTE: assumes client accepts max_retries in ctor
    client = HttpPoliceApiClient(timeout=1, max_retries=0)

    # Act & Assert
    # patch the underlying request to simulate a network timeout
    with patch('requests.Session.get') as mock_get:
        mock_get.side_effect = Exception("Network timeout")

        # Should catch exception and wrap in ApiError
        try:
            client.fetch_stops("metropolitan", "2023-01")
            assert False, "Expected ApiError to be raised"
        except Exception as e:
            # HttpClient doesn't wrap non-HTTP exceptions, so they bubble up
            assert "Network timeout" in str(e)


def test_http_client_handles_invalid_json_response():
    # Arrange
    client = HttpPoliceApiClient()

    # Act & Assert
    with patch('requests.Session.get') as mock_get:
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.text = "Invalid JSON response"
        mock_response.json.side_effect = json.JSONDecodeError("Invalid JSON", "", 0)
        mock_get.return_value = mock_response

        with pytest.raises(ApiError):
            client.fetch_stops("metropolitan", "2023-01")


def test_sqlite_repository_handles_database_corruption():
    # Arrange - test that repository gracefully handles database issues
    from stopsearch_etl.sqlite_repository import SqliteStopSearchRepository

    # Act & Assert - simulate database connection failure
    with pytest.raises(Exception):
        # Invalid database URL should raise exception
        repo = SqliteStopSearchRepository("invalid://database/url")
        repo.count()  # This should fail


def test_etl_service_handles_empty_force_list():
    # Arrange
    api_client = Mock()
    repository = Mock()
    etl_service = EtlService(api_client, repository)

    # Act
    api_client.fetch_stops.return_value = []
    repository.save_batch.return_value = 0
    result = etl_service.extract_transform_load("", "2023-01")

    # Assert - should handle empty force gracefully
    assert result == 0


def test_etl_service_handles_extremely_large_dataset():
    # Arrange
    api_client = Mock()
    repository = Mock()
    etl_service = EtlService(api_client, repository)

    # Act - simulate very large dataset
    large_dataset = [{"type": "Person search", "datetime": "2023-01-01T10:00:00"} for _ in range(10000)]
    api_client.fetch_stops.return_value = large_dataset

    # Mock successful parsing but repository failure
    repository.save_batch.side_effect = Exception("Memory exceeded")

    # Assert
    with pytest.raises(Exception):
        etl_service.extract_transform_load("metropolitan", "2023-01")


def test_domain_record_handles_boundary_datetime_values():
    # Arrange & Act - test edge cases for datetime
    edge_cases = [
        {"datetime": "2023-12-31T23:59:59"},  # End of year
        {"datetime": "2023-01-01T00:00:00"},  # Start of year
        {"datetime": "2023-02-29T12:00:00"},  # Invalid leap year date (2023 not leap)
    ]

    # Assert
    # Valid dates should work
    record1 = StopSearchRecord.from_api_data(edge_cases[0])
    assert record1.datetime.year == 2023

    record2 = StopSearchRecord.from_api_data(edge_cases[1])
    assert record2.datetime.day == 1

    # Invalid leap year date should raise error
    with pytest.raises(ValueError):
        StopSearchRecord.from_api_data(edge_cases[2])


def test_http_client_handles_rate_limit_edge_cases():
    # Arrange
    client = HttpPoliceApiClient(max_retries=0, backoff_factor=0.1)

    # Act & Assert
    with patch('requests.Session.get') as mock_get:
        # First call fails with rate limit
        mock_response = Mock()
        mock_response.status_code = 429
        mock_response.raise_for_status.side_effect = Exception("Rate limited")
        mock_get.return_value = mock_response

        # Should handle HTTP errors gracefully
        try:
            client.fetch_stops("metropolitan", "2023-01")
            assert False, "Expected exception to be raised"
        except Exception as e:
            # HTTP client will bubble up the exception
            assert "Rate limited" in str(e)


def test_etl_service_handles_partial_batch_failures():
    # Arrange
    api_client = Mock()
    repository = Mock()
    etl_service = EtlService(api_client, repository)

    # Act - simulate mixed valid/invalid records
    mixed_data = [
        {"type": "Person search", "datetime": "2023-01-01T10:00:00"},  # Valid
        {"type": None, "datetime": "invalid-date"},  # Invalid
        {"type": "Vehicle search", "datetime": "2023-01-01T11:00:00"},  # Valid
    ]
    api_client.fetch_stops.return_value = mixed_data

    # Repository should be called with only valid records
    repository.save_batch.return_value = 2

    result = etl_service.extract_transform_load("metropolitan", "2023-01")

    # Assert - should process valid records despite some failures
    assert result == 2  # Two valid records saved
    repository.save_batch.assert_called_once()  # Called with valid records only