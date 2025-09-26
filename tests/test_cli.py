import pytest
from unittest.mock import Mock, patch
import sys
from io import StringIO

from stopsearch_etl.cli import main, create_parser


def test_parser_handles_backfill_command(): # basic: backfill with one force
    # Arrange
    parser = create_parser()

    # Act
    args = parser.parse_args(['backfill', '--force', 'metropolitan'])

    # Assert
    assert args.command == 'backfill'
    assert args.force == ['metropolitan']


def test_parser_handles_multiple_forces(): # multiple forces parsed into a list
    # Arrange
    parser = create_parser()

    # Act
    args = parser.parse_args(['backfill', '--force', 'metropolitan', 'avon-and-somerset'])

    # Assert
    assert args.force == ['metropolitan', 'avon-and-somerset']


def test_parser_handles_since_parameter(): # since filter present
    # Arrange
    parser = create_parser()

    # Act
    args = parser.parse_args(['backfill', '--force', 'metropolitan', '--since', '2020-01'])

    # Assert
    assert args.force == ['metropolitan']
    assert args.since == '2020-01'


def test_parser_handles_run_once_command(): # run-now command
    # Arrange
    parser = create_parser()

    # Act
    args = parser.parse_args(['run-once'])

    # Assert
    assert args.command == 'run-once'


@patch('stopsearch_etl.cli.setup_application')
def test_main_executes_backfill_command(mock_setup):
    # Arrange
    mock_backfill_service = Mock()
    mock_multi_runner = Mock()

    from stopsearch_etl.backfill_service import BackfillResult
    mock_result = BackfillResult(
        force="metropolitan",
        total_records=1500,
        months_processed=12,
        months_failed=0
    )
    mock_backfill_service.backfill_force.return_value = mock_result

    mock_setup.return_value = (None, None, mock_backfill_service, mock_multi_runner, None)

    test_args = ['backfill', '--force', 'metropolitan']

    # Act
    with patch.object(sys, 'argv', ['cli.py'] + test_args):
        main()

    # Assert
    mock_backfill_service.backfill_force.assert_called_once_with('metropolitan')


@patch('stopsearch_etl.cli.setup_application')
def test_main_executes_run_once_command(mock_setup):
    # Arrange
    mock_scheduler = Mock()

    from stopsearch_etl.multi_force_runner import MultiForceRunSummary
    mock_summary = MultiForceRunSummary(
        total_records=2000,
        forces_completed=2,
        forces_failed=0
    )
    mock_scheduler.run_once.return_value = mock_summary

    mock_setup.return_value = (None, None, None, None, mock_scheduler)

    test_args = ['run-once']

    # Act
    with patch.object(sys, 'argv', ['cli.py'] + test_args):
        main()

    # Assert
    mock_scheduler.run_once.assert_called_once()


def test_parser_requires_force_for_backfill(): # backfill needs --force
    # Arrange
    parser = create_parser()

    # Act & Assert
    with pytest.raises(SystemExit):
        parser.parse_args(['backfill'])  # Missing --force argument


def test_parser_validates_since_format(): # valid YYYY-MM passes
    # Arrange
    parser = create_parser()

    # Act - should not raise exception for valid format
    args = parser.parse_args(['backfill', '--force', 'metropolitan', '--since', '2023-12'])

    # Assert
    assert args.since == '2023-12'