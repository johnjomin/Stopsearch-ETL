from datetime import datetime
from typing import List
from sqlalchemy import Column, Integer, String, DateTime, Float, Boolean, Text, UniqueConstraint
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import Session
from sqlalchemy.dialects.sqlite import insert

from .domain import StopSearchRecord
from .repository import StopSearchRepository

Base = declarative_base()


class StopSearchTable(Base):
    """SQLAlchemy table model for stop & search records."""
    __tablename__ = 'stop_search_records'

    id = Column(Integer, primary_key=True)
    type = Column(String(100))
    datetime = Column(DateTime)
    gender = Column(String(20))
    age_range = Column(String(20))
    self_defined_ethnicity = Column(String(200))
    officer_defined_ethnicity = Column(String(200))
    legislation = Column(Text)
    object_of_search = Column(String(100))
    outcome = Column(Text)
    outcome_linked_to_object_of_search = Column(Boolean)
    removal_of_more_than_outer_clothing = Column(Boolean)
    latitude = Column(Float)
    longitude = Column(Float)
    street_id = Column(Integer)
    street_name = Column(String(500))

    # TODO: consider adding 'force' if/when you store it (same place/time across forces is possible)
    __table_args__ = (
        UniqueConstraint('datetime', 'latitude', 'longitude', 'type', 'legislation',
                        name='unique_stop_search'),
    )


class SqliteStopSearchRepository(StopSearchRepository):
    """SQLite implementation of the stop search repository."""

    def __init__(self, session: Session):
        self.session = session

    def save(self, record: StopSearchRecord) -> None:
        """Save a single record with upsert behavior."""
        db_record = StopSearchTable(
            type=record.type,
            datetime=record.datetime,
            gender=record.gender,
            age_range=record.age_range,
            self_defined_ethnicity=record.self_defined_ethnicity,
            officer_defined_ethnicity=record.officer_defined_ethnicity,
            legislation=record.legislation,
            object_of_search=record.object_of_search,
            outcome=record.outcome,
            outcome_linked_to_object_of_search=record.outcome_linked_to_object_of_search,
            removal_of_more_than_outer_clothing=record.removal_of_more_than_outer_clothing,
            latitude=record.latitude,
            longitude=record.longitude,
            street_id=record.street_id,
            street_name=record.street_name
        )

        # Use INSERT OR IGNORE for idempotency (SQLite specific)
        stmt = insert(StopSearchTable).values(
            type=record.type,
            datetime=record.datetime,
            gender=record.gender,
            age_range=record.age_range,
            self_defined_ethnicity=record.self_defined_ethnicity,
            officer_defined_ethnicity=record.officer_defined_ethnicity,
            legislation=record.legislation,
            object_of_search=record.object_of_search,
            outcome=record.outcome,
            outcome_linked_to_object_of_search=record.outcome_linked_to_object_of_search,
            removal_of_more_than_outer_clothing=record.removal_of_more_than_outer_clothing,
            latitude=record.latitude,
            longitude=record.longitude,
            street_id=record.street_id,
            street_name=record.street_name
        )

        # Use ON CONFLICT IGNORE for idempotency
        stmt = stmt.on_conflict_do_nothing()
        self.session.execute(stmt)
        self.session.commit()

    def save_batch(self, records: List[StopSearchRecord]) -> int:
        """Save multiple records efficiently."""
        if not records:
            return 0

        # Convert domain objects to dict for bulk insert
        record_dicts = []
        for record in records:
            record_dicts.append({
                'type': record.type,
                'datetime': record.datetime,
                'gender': record.gender,
                'age_range': record.age_range,
                'self_defined_ethnicity': record.self_defined_ethnicity,
                'officer_defined_ethnicity': record.officer_defined_ethnicity,
                'legislation': record.legislation,
                'object_of_search': record.object_of_search,
                'outcome': record.outcome,
                'outcome_linked_to_object_of_search': record.outcome_linked_to_object_of_search,
                'removal_of_more_than_outer_clothing': record.removal_of_more_than_outer_clothing,
                'latitude': record.latitude,
                'longitude': record.longitude,
                'street_id': record.street_id,
                'street_name': record.street_name
            })

        stmt = insert(StopSearchTable).on_conflict_do_nothing()

        # Execute batch insert and track affected rows
        # TODO: if performance matters, try driver rowcount or a changes() call (when supported)
        initial_count = self.session.query(StopSearchTable).count()
        self.session.execute(stmt, record_dicts)
        self.session.commit()
        final_count = self.session.query(StopSearchTable).count()

        return final_count - initial_count

    def find_by_force_and_month(self, force: str, year_month: str) -> List[StopSearchRecord]:
        """Find all records for a specific force and month."""
        # Note: We don't store force in the record yet, so this is a placeholder
        # In a real implementation, we'd filter by force and date range
        return []