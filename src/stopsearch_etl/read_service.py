from typing import List, Dict, Any, Optional
from sqlalchemy import func, extract, and_
from sqlalchemy.orm import Session
from datetime import datetime
import math

from .domain import StopSearchRecord
from .repository import StopSearchRepository
from .sqlite_repository import StopSearchTable


class ReadService:
    """Simple read/query layer for stored stop & search data"""

    def __init__(self, repository: StopSearchRepository):
        self.repository = repository
        # if repo exposes a SQLAlchemy session, use it for richer queries
        # TODO: consider making session a hard requirement or pass it in directly
        if hasattr(repository, 'session'):
            self.session = repository.session
        else:
            self.session = None

    def get_records_by_month(self, year_month: str, limit: Optional[int] = None) -> List[StopSearchRecord]:
        """
        Get records for a given month (YYYY-MM)

        Returns:
            List of StopSearchRecord objects
        """
        if not self.session:
            # fallback: use repo method if we don't have a session
            # NOTE: force not stored yet, so pass empty string
            return self.repository.find_by_force_and_month("", year_month)

        try:
            year, month = year_month.split('-')
            year, month = int(year), int(month)
        except ValueError:
            return []

        # filter by year and month
        # TODO: add an index on datetime for faster month queries
        query = self.session.query(StopSearchTable).filter(
            and_(
                extract('year', StopSearchTable.datetime) == year,
                extract('month', StopSearchTable.datetime) == month
            )
        )

        if limit:
            query = query.limit(limit)

        db_records = query.all()
        return [self._db_record_to_domain(record) for record in db_records]

    def get_records_by_outcome(self, outcome: str) -> List[StopSearchRecord]:
        """Get all records with this outcome"""
        if not self.session:
            return []

        db_records = self.session.query(StopSearchTable).filter(
            StopSearchTable.outcome == outcome
        ).all()

        return [self._db_record_to_domain(record) for record in db_records]

    def get_records_by_type(self, search_type: str) -> List[StopSearchRecord]:
        """Get all records for this search type"""
        if not self.session:
            return []

        db_records = self.session.query(StopSearchTable).filter(
            StopSearchTable.type == search_type
        ).all()

        return [self._db_record_to_domain(record) for record in db_records]

    def get_summary_stats(self) -> Dict[str, Any]:
        """Basic counts: total, by type, by outcome"""
        if not self.session:
            return {"total_records": 0}

        # total
        total_records = self.session.query(StopSearchTable).count()

        # count by type
        search_type_counts = self.session.query(
            StopSearchTable.type,
            func.count(StopSearchTable.type)
        ).group_by(StopSearchTable.type).all()

        search_types = {search_type: count for search_type, count in search_type_counts}

        # counts by outcome (skip NULL)
        outcome_counts = self.session.query(
            StopSearchTable.outcome,
            func.count(StopSearchTable.outcome)
        ).filter(StopSearchTable.outcome.isnot(None)).group_by(StopSearchTable.outcome).all()

        outcomes = {outcome: count for outcome, count in outcome_counts}

        return {
            "total_records": total_records,
            "search_types": search_types,
            "outcomes": outcomes
        }

    def get_records_near_location(self, lat: float, lon: float, radius_km: float = 1.0) -> List[StopSearchRecord]:
        """
        Get records near a lat/lon within a small radius (km)

        Returns:
            List of StopSearchRecord objects within the radius
        """
        if not self.session:
            return []

        # rough bounding box (good enough for small distances)
        # TODO: for real geo queries, use PostGIS or geodesic distance
        lat_delta = radius_km / 111.0  # km per degree conversion
        lon_delta = radius_km / (111.0 * math.cos(math.radians(lat)))

        db_records = self.session.query(StopSearchTable).filter(
            and_(
                StopSearchTable.latitude.between(lat - lat_delta, lat + lat_delta),
                StopSearchTable.longitude.between(lon - lon_delta, lon + lon_delta),
                StopSearchTable.latitude.isnot(None),
                StopSearchTable.longitude.isnot(None)
            )
        ).all()

        return [self._db_record_to_domain(record) for record in db_records]

    def _db_record_to_domain(self, db_record: StopSearchTable) -> StopSearchRecord:
        """Map DB row to domain object"""
        return StopSearchRecord(
            type=db_record.type or "Unknown",
            datetime=db_record.datetime,
            gender=db_record.gender,
            age_range=db_record.age_range,
            self_defined_ethnicity=db_record.self_defined_ethnicity,
            officer_defined_ethnicity=db_record.officer_defined_ethnicity,
            legislation=db_record.legislation or "",
            object_of_search=db_record.object_of_search,
            outcome=db_record.outcome,
            outcome_linked_to_object_of_search=db_record.outcome_linked_to_object_of_search,
            removal_of_more_than_outer_clothing=db_record.removal_of_more_than_outer_clothing,
            latitude=db_record.latitude,
            longitude=db_record.longitude,
            street_id=db_record.street_id,
            street_name=db_record.street_name
        )