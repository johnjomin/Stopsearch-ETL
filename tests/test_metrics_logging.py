import pytest
from unittest.mock import Mock, patch
from dataclasses import asdict

from stopsearch_etl.metrics import MetricsCollector, EtlMetrics
from stopsearch_etl.etl_service import EtlService


def test_metrics_collector_tracks_successful_records(): # two good batches -> total adds up
    # Arrange
    collector = MetricsCollector()

    # Act
    collector.record_successful_batch("metropolitan", "2023-01", 150, 0)
    collector.record_successful_batch("avon-and-somerset", "2023-02", 75, 10)

    # Assert
    metrics = collector.get_current_metrics()
    assert metrics.total_records_ingested == 225  # 150 + 75
    assert metrics.total_records_deduplicated == 10
    assert metrics.total_batches_processed == 2
    assert metrics.total_batches_failed == 0


def test_metrics_collector_tracks_failed_batches(): # one success + one failure -> counts and failure list updated
    # Arrange
    collector = MetricsCollector()

    # Act
    collector.record_successful_batch("metropolitan", "2023-01", 100, 5)
    collector.record_failed_batch("avon-and-somerset", "2023-02", "API timeout")

    # Assert
    metrics = collector.get_current_metrics()
    assert metrics.total_records_ingested == 100
    assert metrics.total_records_deduplicated == 5
    assert metrics.total_batches_processed == 1
    assert metrics.total_batches_failed == 1
    assert len(metrics.failed_batches) == 1
    assert "avon-and-somerset" in metrics.failed_batches[0]["batch_id"]


def test_metrics_collector_can_reset():
    # Arrange
    collector = MetricsCollector()
    collector.record_successful_batch("metropolitan", "2023-01", 100, 0)

    # Act
    collector.reset()

    # Assert
    metrics = collector.get_current_metrics()
    assert metrics.total_records_ingested == 0
    assert metrics.total_batches_processed == 0


@patch('stopsearch_etl.metrics.logger')
def test_metrics_collector_logs_structured_data(mock_logger):
    # Arrange
    collector = MetricsCollector()

    # Act
    collector.record_successful_batch("metropolitan", "2023-01", 150, 10)

    # Assert
    mock_logger.info.assert_called_once()
    log_call = mock_logger.info.call_args
    assert "Batch completed successfully" in log_call[0][0]

    # Verify structured data in log call kwargs (extra parameter)
    log_kwargs = log_call[1]
    extra_data = log_kwargs["extra"]
    assert extra_data["force"] == "metropolitan"
    assert extra_data["month"] == "2023-01"
    assert extra_data["records_ingested"] == 150
    assert extra_data["records_deduplicated"] == 10
    # TODO: also test the error path (logger.error) with record_failed_batch


def test_etl_service_with_metrics_integration():
    # Arrange
    mock_api_client = Mock()
    mock_repository = Mock()
    mock_metrics = Mock()

    api_response = [
        {
            "type": "Person search",
            "datetime": "2023-01-15T14:30:00+00:00",
            "legislation": "Police Act"
        }
    ]

    mock_api_client.fetch_stops.return_value = api_response
    mock_repository.save_batch.return_value = 1

    etl_service = EtlService(mock_api_client, mock_repository, mock_metrics)

    # Act
    result = etl_service.extract_transform_load("metropolitan", "2023-01")

    # Assert
    assert result == 1
    mock_metrics.record_successful_batch.assert_called_once_with(
        "metropolitan", "2023-01", 1, 0
    )
    # TODO: add a test for the failure path (record_failed_batch) by making repo throw