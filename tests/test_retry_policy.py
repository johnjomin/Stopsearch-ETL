import pytest
import responses
from unittest.mock import patch
import time

from stopsearch_etl.api import ApiError
from stopsearch_etl.http_client import HttpPoliceApiClient


@responses.activate
def test_http_client_retries_on_server_errors(): # 500 twice, then 200 -> should retry and succeeed
    # Arrange
    force = "metropolitan"
    year_month = "2023-01"
    expected_url = f"https://data.police.uk/api/stops-force?force={force}&date={year_month}"

    # First two requests fail with 500, third succeeds
    responses.add(responses.GET, expected_url, status=500)
    responses.add(responses.GET, expected_url, status=500)
    responses.add(responses.GET, expected_url, json=[{"type": "Person search"}], status=200)

    client = HttpPoliceApiClient(max_retries=3, backoff_factor=0.1)

    # Act
    result = client.fetch_stops(force, year_month)

    # Assert
    assert len(result) == 1
    assert result[0]["type"] == "Person search"
    # Should have made 3 requests total (2 failures + 1 success)
    assert len(responses.calls) == 3


@responses.activate
def test_http_client_retries_on_rate_limiting(): # 429 then 200 -> one rety
    # Arrange
    force = "metropolitan"
    year_month = "2023-01"
    expected_url = f"https://data.police.uk/api/stops-force?force={force}&date={year_month}"

    # First request hits rate limit, second succeeds
    responses.add(responses.GET, expected_url, status=429)
    responses.add(responses.GET, expected_url, json=[{"type": "Vehicle search"}], status=200)

    client = HttpPoliceApiClient(max_retries=2, backoff_factor=0.1)

    # Act
    result = client.fetch_stops(force, year_month)

    # Assert
    assert len(result) == 1
    assert result[0]["type"] == "Vehicle search"
    assert len(responses.calls) == 2


@responses.activate
def test_http_client_gives_up_after_max_retries():
    # Arrange
    force = "metropolitan"
    year_month = "2023-01"
    expected_url = f"https://data.police.uk/api/stops-force?force={force}&date={year_month}"

    # All requests fail with 503
    for _ in range(5):
        responses.add(responses.GET, expected_url, status=503)

    client = HttpPoliceApiClient(max_retries=3, backoff_factor=0.1)

    # Act & Assert
    with pytest.raises(ApiError) as exc_info:
        client.fetch_stops(force, year_month)

    assert "503" in str(exc_info.value)
    # Should have made 4 requests total (1 initial + 3 retries)
    assert len(responses.calls) == 4


@responses.activate
def test_http_client_does_not_retry_4xx_errors():
    # Arrange
    force = "nonexistent-force"
    year_month = "2023-01"
    expected_url = f"https://data.police.uk/api/stops-force?force={force}&date={year_month}"

    # Return 404 - should not be retried
    responses.add(responses.GET, expected_url, status=404)

    client = HttpPoliceApiClient(max_retries=3, backoff_factor=0.1)

    # Act & Assert
    with pytest.raises(ApiError) as exc_info:
        client.fetch_stops(force, year_month)

    assert "404" in str(exc_info.value)
    # Should have made only 1 request (no retries for 404)
    assert len(responses.calls) == 1


def test_http_client_configurable_retry_parameters():
    # Arrange & Act
    client = HttpPoliceApiClient(max_retries=5, backoff_factor=2.0, timeout=60)

    # Assert
    # Check that the client was initialized with custom parameters
    assert client.timeout == 60
    assert client.max_retries == 5
    assert client.backoff_factor == 2.0


def test_http_client_retry_strategy_configuration(): # basic retry strategy wiring looks right
    # Arrange & Act
    client = HttpPoliceApiClient(max_retries=2, backoff_factor=0.5)

    # Assert
    # Verify that the retry strategy is configured correctly
    adapter = client.session.get_adapter('https://')
    retry_strategy = adapter.max_retries

    assert retry_strategy.total == 2
    assert retry_strategy.backoff_factor == 0.5
    assert 429 in retry_strategy.status_forcelist
    assert 500 in retry_strategy.status_forcelist
    assert 502 in retry_strategy.status_forcelist
    assert 503 in retry_strategy.status_forcelist
    assert 504 in retry_strategy.status_forcelist