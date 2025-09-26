import pytest
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from stopsearch_etl.domain import StopSearchRecord
from stopsearch_etl.sqlite_repository import SqliteStopSearchRepository, Base
from stopsearch_etl.etl_service import EtlService


@pytest.fixture
def in_memory_repo():
    """Create an in-memory SQLite repository for testing."""
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    repo = SqliteStopSearchRepository(session)
    yield repo
    session.close()


def test_repository_prevents_duplicate_records_on_repeated_saves(in_memory_repo):
    # Arrange
    repo = in_memory_repo
    record = StopSearchRecord(
        type="Person search",
        datetime=datetime(2023, 1, 15, 14, 30),
        gender="Male",
        age_range="25-34",
        self_defined_ethnicity=None,
        officer_defined_ethnicity="White",
        legislation="Police Act",
        object_of_search="Drugs",
        outcome="No action",
        outcome_linked_to_object_of_search=False,
        removal_of_more_than_outer_clothing=False,
        latitude=51.5074,
        longitude=-0.1278,
        street_id=883407,
        street_name="High Street"
    )

    # Act - save the same record multiple times
    result1 = repo.save_batch([record])
    result2 = repo.save_batch([record])
    result3 = repo.save_batch([record])

    # Assert
    assert result1 == 1  # First save should insert 1 record
    assert result2 == 0  # Second save should be ignored (duplicate)
    assert result3 == 0  # Third save should be ignored (duplicate)


def test_etl_service_produces_idempotent_results():
    # Arrange
    from unittest.mock import Mock

    mock_api_client = Mock()
    mock_repository = Mock()

    # Same API response data
    api_response = [
        {
            "type": "Person search",
            "datetime": "2023-01-15T14:30:00+00:00",
            "gender": "Male",
            "age_range": "25-34",
            "legislation": "Police Act",
            "object_of_search": "Drugs",
            "outcome": "No action",
            "outcome_linked_to_object_of_search": False,
            "removal_of_more_than_outer_clothing": False,
            "location": {
                "latitude": 51.5074,
                "longitude": -0.1278,
                "street": {"id": 883407, "name": "High Street"}
            }
        }
    ]

    mock_api_client.fetch_stops.return_value = api_response
    mock_repository.save_batch.side_effect = [1, 0, 0]  # First succeeds, rest are duplicates

    etl_service = EtlService(mock_api_client, mock_repository)

    # Act - run ETL multiple times with same data
    result1 = etl_service.extract_transform_load("metropolitan", "2023-01")
    result2 = etl_service.extract_transform_load("metropolitan", "2023-01")
    result3 = etl_service.extract_transform_load("metropolitan", "2023-01")

    # Assert
    assert result1 == 1  # First run inserts data
    assert result2 == 0  # Second run finds duplicates
    assert result3 == 0  # Third run finds duplicates

    # API should have been called each time (no caching)
    assert mock_api_client.fetch_stops.call_count == 3

    # Repository should have been called each time with the same data
    assert mock_repository.save_batch.call_count == 3


def test_sqlite_repository_handles_similar_but_different_records(in_memory_repo):
    # Arrange
    repo = in_memory_repo

    # Two records that are similar but differ in key fields
    record1 = StopSearchRecord(
        type="Person search",
        datetime=datetime(2023, 1, 15, 14, 30),  # Same time
        gender="Male",
        age_range="25-34",
        self_defined_ethnicity=None,
        officer_defined_ethnicity="White",
        legislation="Police Act",
        object_of_search="Drugs",
        outcome="No action",
        outcome_linked_to_object_of_search=False,
        removal_of_more_than_outer_clothing=False,
        latitude=51.5074,  # Same location
        longitude=-0.1278,
        street_id=883407,
        street_name="High Street"
    )

    record2 = StopSearchRecord(
        type="Person search",
        datetime=datetime(2023, 1, 15, 14, 31),  # Different time (1 minute later)
        gender="Male",
        age_range="25-34",
        self_defined_ethnicity=None,
        officer_defined_ethnicity="White",
        legislation="Police Act",
        object_of_search="Drugs",
        outcome="No action",
        outcome_linked_to_object_of_search=False,
        removal_of_more_than_outer_clothing=False,
        latitude=51.5074,  # Same location
        longitude=-0.1278,
        street_id=883407,
        street_name="High Street"
    )

    # Act
    result1 = repo.save_batch([record1])
    result2 = repo.save_batch([record2])

    # Assert
    assert result1 == 1  # First record should be saved
    assert result2 == 1  # Second record should also be saved (different datetime)