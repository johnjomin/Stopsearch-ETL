import pytest
import responses
from stopsearch_etl.api import ApiError
from stopsearch_etl.http_client import HttpPoliceApiClient


@responses.activate
def test_http_client_fetches_available_months_successfully():
    # Arrange
    force = "metropolitan"
    expected_url = f"https://data.police.uk/api/stops-force?force={force}"

    mock_response = [
        {
            "date": "2023-12",
            "stop-and-search": ["metropolitan"]
        },
        {
            "date": "2023-11",
            "stop-and-search": ["metropolitan"]
        },
        {
            "date": "2023-10",
            "stop-and-search": ["metropolitan"]
        }
    ]

    responses.add(
        responses.GET,
        expected_url,
        json=mock_response,
        status=200
    )

    client = HttpPoliceApiClient()

    # Act
    result = client.get_available_months(force)

    # Assert
    assert len(result) == 3
    assert "2023-12" in result
    assert "2023-11" in result
    assert "2023-10" in result


@responses.activate
def test_http_client_filters_months_with_stop_search_data():
    # Arrange
    force = "metropolitan"
    expected_url = f"https://data.police.uk/api/stops-force?force={force}"

    mock_response = [
        {
            "date": "2023-12",
            "stop-and-search": ["metropolitan"]
        },
        {
            "date": "2023-11",
            "crime": ["metropolitan"]  # This month has crime but no stop-and-search
        }
    ]

    responses.add(
        responses.GET,
        expected_url,
        json=mock_response,
        status=200
    )

    client = HttpPoliceApiClient()

    # Act
    result = client.get_available_months(force)

    # Assert
    assert len(result) == 1
    assert "2023-12" in result
    assert "2023-11" not in result