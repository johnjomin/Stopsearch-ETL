from abc import ABC
import pytest
from stopsearch_etl.api import PoliceApiClient, ApiError


def test_police_api_client_is_abstract():
    # Arrange & Act & Assert
    with pytest.raises(TypeError): # You shouldn’t be able to just spin up PoliceApiClient directly
        PoliceApiClient()


def test_police_api_client_defines_fetch_stops_method():
    # Arrange
    class TestClient(PoliceApiClient): # Make a dummy subclass that pretends to fetch stuff
        def fetch_stops(self, force: str, year_month: str) -> list[dict]:
            return []

    # Act
    client = TestClient()

    # Assert
    # should have the fetch_stops method
    assert hasattr(client, 'fetch_stops')
    assert callable(getattr(client, 'fetch_stops'))


def test_api_error_exception_exists():
    # Arrange
    error_message = "API rate limit exceeded"

    # Act
    error = ApiError(error_message)

    # Assert
    assert str(error) == error_message
    assert isinstance(error, Exception)