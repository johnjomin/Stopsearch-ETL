from abc import ABC
import pytest
from datetime import datetime
from stopsearch_etl.domain import StopSearchRecord
from stopsearch_etl.repository import StopSearchRepository


def test_repository_is_abstract():
    # Arrange & Act & Assert
    # shouldn't be able to create the abstract base directly
    with pytest.raises(TypeError):
        StopSearchRepository()


def test_repository_defines_save_method():
    # Arrange
    class TestRepository(StopSearchRepository):
        def save(self, record: StopSearchRecord) -> None:
            pass

        def save_batch(self, records: list[StopSearchRecord]) -> int:
            return len(records)

        def find_by_force_and_month(self, force: str, year_month: str) -> list[StopSearchRecord]:
            return []

    # Act
    repo = TestRepository()

    # Assert
    assert hasattr(repo, 'save')
    assert hasattr(repo, 'save_batch')
    assert hasattr(repo, 'find_by_force_and_month')
    assert callable(getattr(repo, 'save'))


def test_repository_defines_batch_save_method():
    # Arrange
    record1 = StopSearchRecord(
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
        latitude=51.5,
        longitude=-0.1,
        street_id=123,
        street_name="High Street"
    )

    class TestRepository(StopSearchRepository):
        def save(self, record: StopSearchRecord) -> None:
            pass

        def save_batch(self, records: list[StopSearchRecord]) -> int:
            return len(records)

        def find_by_force_and_month(self, force: str, year_month: str) -> list[StopSearchRecord]:
            return []

    repo = TestRepository()

    # Act
    result = repo.save_batch([record1])

    # Assert
    assert result == 1