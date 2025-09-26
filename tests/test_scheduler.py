import pytest
from unittest.mock import Mock, patch
from datetime import time

from stopsearch_etl.scheduler import EtlScheduler


def test_scheduler_initializes_with_default_schedule(): # default time is 02:00
    # Arrange
    mock_runner = Mock()
    forces = ["metropolitan", "avon-and-somerset"]

    # Act
    scheduler = EtlScheduler(mock_runner, forces)

    # Assert
    assert scheduler.forces == forces
    assert scheduler.schedule_time == time(2, 0)  # Default 2 AM
    assert scheduler.multi_force_runner == mock_runner


def test_scheduler_initializes_with_custom_schedule(): # custom time is used
    # Arrange
    mock_runner = Mock()
    forces = ["metropolitan"]
    custom_time = time(6, 30)

    # Act
    scheduler = EtlScheduler(mock_runner, forces, schedule_time=custom_time)

    # Assert
    assert scheduler.schedule_time == custom_time


@patch('stopsearch_etl.scheduler.BackgroundScheduler')
def test_scheduler_starts_background_job(mock_scheduler_class):
    # Arrange
    mock_scheduler_instance = Mock()
    mock_scheduler_class.return_value = mock_scheduler_instance

    mock_runner = Mock()
    forces = ["metropolitan"]

    scheduler = EtlScheduler(mock_runner, forces)

    # Act
    scheduler.start()

    # Assert
    mock_scheduler_instance.add_job.assert_called_once()
    mock_scheduler_instance.start.assert_called_once()

    # Verify job configuration
    job_call = mock_scheduler_instance.add_job.call_args
    # Check keyword arguments
    assert job_call[1]['func'] == scheduler._run_etl_job  # Function to run
    assert job_call[1]['trigger'] == 'cron'
    assert job_call[1]['hour'] == 2
    assert job_call[1]['minute'] == 0


@patch('stopsearch_etl.scheduler.BackgroundScheduler')
def test_scheduler_stops_background_job(mock_scheduler_class):
    # Arrange
    mock_scheduler_instance = Mock()
    mock_scheduler_class.return_value = mock_scheduler_instance

    mock_runner = Mock()
    forces = ["metropolitan"]

    scheduler = EtlScheduler(mock_runner, forces)
    scheduler.start()

    # Act
    scheduler.stop()

    # Assert
    mock_scheduler_instance.shutdown.assert_called_once()


def test_scheduler_run_once_executes_immediately():
    # Arrange
    mock_runner = Mock()
    forces = ["metropolitan", "avon-and-somerset"]

    # Mock successful run
    from stopsearch_etl.multi_force_runner import MultiForceRunSummary
    mock_summary = MultiForceRunSummary(
        total_records=1500,
        forces_completed=2,
        forces_failed=0
    )
    mock_runner.run_backfill.return_value = mock_summary

    scheduler = EtlScheduler(mock_runner, forces)

    # Act
    result = scheduler.run_once()

    # Assert
    assert result == mock_summary
    mock_runner.run_backfill.assert_called_once_with(forces)


def test_scheduler_run_once_handles_failures():
    # Arrange
    mock_runner = Mock()
    forces = ["metropolitan"]

    # Mock failed run
    mock_runner.run_backfill.side_effect = Exception("Database connection failed")

    scheduler = EtlScheduler(mock_runner, forces)

    # Act & Assert
    with pytest.raises(Exception) as exc_info:
        scheduler.run_once()

    assert "Database connection failed" in str(exc_info.value)
    mock_runner.run_backfill.assert_called_once_with(forces)