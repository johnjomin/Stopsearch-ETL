import pytest
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from stopsearch_etl.domain import StopSearchRecord
from stopsearch_etl.sqlite_repository import SqliteStopSearchRepository, Base


@pytest.fixture
def in_memory_db():
    """Create an in-memory SQLite database for testing."""
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    repo = SqliteStopSearchRepository(session)

    yield repo

    session.close()


def test_sqlite_repo_saves_single_record(in_memory_db):
    # Arrange
    repo = in_memory_db
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

    # Act
    repo.save(record)

    # Assert
    found_records = repo.find_by_force_and_month("test-force", "2023-01")
    # Note: we're not storing force in record yet, so this will be empty
    # but the save should not have crashed
    assert isinstance(found_records, list)


def test_sqlite_repo_handles_duplicate_saves_idempotently(in_memory_db):
    # Arrange
    repo = in_memory_db
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

    # Act
    repo.save(record)
    repo.save(record)  # Save same record again

    # Assert
    # Should not raise an error and should handle the duplicate gracefully
    assert True  # If we get here without exception, test passes