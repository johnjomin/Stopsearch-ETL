from typing import List

from .api import PoliceApiClient, ApiError
from .domain import StopSearchRecord
from .repository import StopSearchRepository


class EtlService:
    """Service for extracting, transforming, and loading stop search data"""

    # pull from API -> map to domain -> save
    def __init__(self, api_client: PoliceApiClient, repository: StopSearchRepository):
        self.api_client = api_client
        self.repository = repository

    def extract_transform_load(self, force: str, year_month: str) -> int:
        """
        Extract data from API, transform to objects and load to repository

        Returns:
            Number of records successfully saved
        """
        # Extract: Get raw data from API
        raw_records = self.api_client.fetch_stops(force, year_month)

        if not raw_records:
            # still call repo for consistency
            return self.repository.save_batch([])

        # Transform: map raw -> domain, skip bad ones
        domain_records = []
        for raw_record in raw_records:
            try:
                domain_record = StopSearchRecord.from_api_data(raw_record)
                domain_records.append(domain_record)
            except (KeyError, ValueError) as e:
                # todo: use proper logging
                print(f"Warning: Failed to parse record: {e}")
                continue

        # Load: save
        return self.repository.save_batch(domain_records)