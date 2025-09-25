import pytest
from unittest.mock import Mock

from stopsearch_etl.multi_force_runner import MultiForceRunner
from stopsearch_etl.backfill_service import BackfillResult


def test_multi_force_runner_processes_all_forces(): # happy path: both forces succeed, results roll up as expected
    # Arrange
    mock_backfill_service = Mock()

    # fake per-force summaries
    metropolitan_result = BackfillResult(
        force="metropolitan",
        total_records=1500,
        months_processed=12,
        months_failed=0
    )

    avon_result = BackfillResult(
        force="avon-and-somerset",
        total_records=800,
        months_processed=10,
        months_failed=1
    )

    mock_backfill_service.backfill_force.side_effect = [metropolitan_result, avon_result]

    forces = ["metropolitan", "avon-and-somerset"]
    runner = MultiForceRunner(mock_backfill_service)

    # Act
    summary = runner.run_backfill(forces)

    # Assert
    assert summary.total_records == 2300  # 1500 + 800
    assert summary.total_months_processed == 22  # 12 + 10
    assert summary.total_months_failed == 1
    assert summary.forces_completed == 2
    assert summary.forces_failed == 0

    # make sure each force was attempted, in order
    expected_calls = [
        (("metropolitan",),),
        (("avon-and-somerset",),)
    ]
    assert mock_backfill_service.backfill_force.call_args_list == expected_calls
    # TODO: add a test for duplicate forces in the list (should we de-dupe?)


def test_multi_force_runner_continues_on_force_failure(): # one force fails in the middle; others still processed
    # Arrange
    mock_backfill_service = Mock()

    # First force succeeds, second fails, third succeeds
    successful_result = BackfillResult(
        force="metropolitan",
        total_records=1000,
        months_processed=5,
        months_failed=0
    )

    another_successful_result = BackfillResult(
        force="west-midlands",
        total_records=600,
        months_processed=3,
        months_failed=0
    )

    def backfill_side_effect(force):
        if force == "avon-and-somerset":
            raise Exception("Force API unavailable")
        elif force == "metropolitan":
            return successful_result
        else:
            return another_successful_result

    mock_backfill_service.backfill_force.side_effect = backfill_side_effect

    forces = ["metropolitan", "avon-and-somerset", "west-midlands"]
    runner = MultiForceRunner(mock_backfill_service)

    # Act
    summary = runner.run_backfill(forces)

    # Assert
    assert summary.total_records == 1600  # 1000 + 0 (failed) + 600
    assert summary.forces_completed == 2
    assert summary.forces_failed == 1
    assert len(summary.failed_forces) == 1
    assert "avon-and-somerset" in summary.failed_forces

    # All forces should have been attempted
    assert mock_backfill_service.backfill_force.call_count == 3
    # TODO: add a variant where the first or last force fails (edge ordering)


def test_multi_force_runner_handles_empty_force_list():  # empty input → quick exit, no calls
    # Arrange
    mock_backfill_service = Mock()
    runner = MultiForceRunner(mock_backfill_service)

    # Act
    summary = runner.run_backfill([])

    # Assert
    assert summary.total_records == 0
    assert summary.forces_completed == 0
    assert summary.forces_failed == 0
    mock_backfill_service.backfill_force.assert_not_called() # no work should be dispatched