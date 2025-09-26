import pytest
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from stopsearch_etl.domain import StopSearchRecord
from stopsearch_etl.sqlite_repository import SqliteStopSearchRepository, Base
from stopsearch_etl.read_service import ReadService


@pytest.fixture
def setup_read_service_with_data():
    """Setup read service with sample data for testing."""
    # Build a tiny in-memory DB with sample rows for tests
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    # Create repository and read service
    repository = SqliteStopSearchRepository(session)
    read_service = ReadService(repository)

    # Add sample data
    records = [
        StopSearchRecord(
            type="Person search",
            datetime=datetime(2023, 1, 15, 14, 30),
            gender="Male",
            age_range="25-34",
            self_defined_ethnicity="White - English",
            officer_defined_ethnicity="White",
            legislation="Police Act 2023",
            object_of_search="Drugs",
            outcome="No action",
            outcome_linked_to_object_of_search=False,
            removal_of_more_than_outer_clothing=False,
            latitude=51.5074,
            longitude=-0.1278,
            street_id=883407,
            street_name="High Street"
        ),
        StopSearchRecord(
            type="Vehicle search",
            datetime=datetime(2023, 2, 10, 16, 45),
            gender="Female",
            age_range="18-24",
            self_defined_ethnicity="Black - African",
            officer_defined_ethnicity="Black",
            legislation="Police Act 2023",
            object_of_search="Weapons",
            outcome="Arrest",
            outcome_linked_to_object_of_search=True,
            removal_of_more_than_outer_clothing=False,
            latitude=51.4994,
            longitude=-0.1245,
            street_id=883408,
            street_name="Main Road"
        ),
        StopSearchRecord(
            type="Person search",
            datetime=datetime(2023, 1, 20, 10, 15),
            gender="Male",
            age_range="35-44",
            self_defined_ethnicity="Asian - Pakistani",
            officer_defined_ethnicity="Asian",
            legislation="Police Act 2023",
            object_of_search="Stolen goods",
            outcome="Community resolution",
            outcome_linked_to_object_of_search=True,
            removal_of_more_than_outer_clothing=False,
            latitude=51.5100,
            longitude=-0.1300,
            street_id=883409,
            street_name="Park Lane"
        )
    ]

    repository.save_batch(records)

    yield read_service, repository
    session.close()


def test_read_service_gets_records_by_month(setup_read_service_with_data): # pull january rows
    # Arrange
    read_service, repository = setup_read_service_with_data

    # Act
    january_records = read_service.get_records_by_month("2023-01")

    # Assert
    assert len(january_records) == 2  # Two records from January
    assert all(record.datetime.month == 1 for record in january_records)
    assert all(record.datetime.year == 2023 for record in january_records)


def test_read_service_gets_records_by_month_with_limit(setup_read_service_with_data): # limit to 1
    # Arrange
    read_service, repository = setup_read_service_with_data

    # Act
    january_records = read_service.get_records_by_month("2023-01", limit=1)

    # Assert
    assert len(january_records) == 1
    assert january_records[0].datetime.month == 1


def test_read_service_gets_records_by_outcome_type(setup_read_service_with_data):
    # Arrange
    read_service, repository = setup_read_service_with_data

    # Act
    arrest_records = read_service.get_records_by_outcome("Arrest")

    # Assert
    assert len(arrest_records) == 1
    assert arrest_records[0].outcome == "Arrest"
    assert arrest_records[0].gender == "Female"


def test_read_service_gets_records_by_search_type(setup_read_service_with_data):
    # Arrange
    read_service, repository = setup_read_service_with_data

    # Act
    person_searches = read_service.get_records_by_type("Person search")

    # Assert
    assert len(person_searches) == 2
    assert all(record.type == "Person search" for record in person_searches)


def test_read_service_gets_summary_statistics(setup_read_service_with_data):
    # Arrange
    read_service, repository = setup_read_service_with_data

    # Act
    stats = read_service.get_summary_stats()

    # Assert
    assert stats["total_records"] == 3
    assert stats["search_types"]["Person search"] == 2
    assert stats["search_types"]["Vehicle search"] == 1
    assert stats["outcomes"]["No action"] == 1
    assert stats["outcomes"]["Arrest"] == 1
    assert stats["outcomes"]["Community resolution"] == 1


def test_read_service_handles_empty_results(setup_read_service_with_data):
    # Arrange
    read_service, repository = setup_read_service_with_data

    # Act
    march_records = read_service.get_records_by_month("2023-03")

    # Assert
    assert len(march_records) == 0


def test_read_service_gets_records_by_location_proximity(setup_read_service_with_data): # nearby search finds at least one row
    # Arrange
    read_service, repository = setup_read_service_with_data

    # Act
    # Search near London coordinates (51.5074, -0.1278)
    nearby_records = read_service.get_records_near_location(51.51, -0.13, radius_km=2.0)

    # Assert
    assert len(nearby_records) >= 1  # Should find at least some records
    # All returned records should have coordinates
    assert all(record.latitude is not None and record.longitude is not None
              for record in nearby_records)