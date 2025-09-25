import pytest
import responses
from stopsearch_etl.api import ApiError
from stopsearch_etl.http_client import HttpPoliceApiClient


@responses.activate
def test_http_client_fetches_stops_successfully(): # Pretend API returns a valid stop search record
    # Arrange
    force = "metropolitan"
    year_month = "2023-01"
    expected_url = f"https://data.police.uk/api/stops-force?force={force}&date={year_month}"

    mock_response = [
        {
            "type": "Person search",
            "datetime": "2023-01-15T14:30:00+00:00",
            "gender": "Male",
            "legislation": "Police and Criminal Evidence Act 1984 (section 1)"
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
    result = client.fetch_stops(force, year_month)

    # Assert
    assert len(result) == 1
    assert result[0]["type"] == "Person search"
    assert result[0]["gender"] == "Male"


@responses.activate
def test_http_client_raises_api_error_on_404(): # Pretend API gives us a 404 -> should raise ApiError
    # Arrange
    force = "nonexistent-force"
    year_month = "2023-01"
    expected_url = f"https://data.police.uk/api/stops-force?force={force}&date={year_month}"

    responses.add(
        responses.GET,
        expected_url,
        status=404
    )

    client = HttpPoliceApiClient()

    # Act & Assert
    with pytest.raises(ApiError) as exc_info:
        client.fetch_stops(force, year_month)

    assert "404" in str(exc_info.value)