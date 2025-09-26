from typing import List, Optional

from .api import PoliceApiClient, ApiError
from .domain import StopSearchRecord
from .repository import StopSearchRepository
from .metrics import MetricsCollector


class EtlService:
    """ETL: get from API → turn into objects → save to DB"""

    # pull from API -> map to domain -> save
    def __init__(self, api_client: PoliceApiClient, repository: StopSearchRepository,
                 metrics_collector: Optional[MetricsCollector] = None):
        self.api_client = api_client
        self.repository = repository
        self.metrics_collector = metrics_collector #optional

    def extract_transform_load(self, force: str, year_month: str) -> int:
        """
        Extract data from API, transform to objects and load to repository

        Returns:
            Number of records saved
        """
        try:
            # Extract: Get raw data from API
            raw_records = self.api_client.fetch_stops(force, year_month)

            if not raw_records:
                # still call repo for consistency
                result = self.repository.save_batch([])
                if self.metrics_collector:
                    self.metrics_collector.record_successful_batch(force, year_month, 0, 0)
                return result

            # Transform: map raw -> domain, skip bad ones
            domain_records = []
            for raw_record in raw_records:
                try:
                    domain_record = StopSearchRecord.from_api_data(raw_record)
                    domain_records.append(domain_record)
                except (KeyError, ValueError) as e:
                    # TODO: use logger instead of print
                    print(f"Warning: Failed to parse record: {e}")
                    continue

            # Load: save
            original_count = len(domain_records)
            saved_count = self.repository.save_batch(domain_records)
            deduplicated_count = original_count - saved_count

            # Record metrics if collector is available
            if self.metrics_collector:
                self.metrics_collector.record_successful_batch(
                    force, year_month, saved_count, deduplicated_count
                )

            return saved_count

        except Exception as e:
            # Record failure metrics
            # TODO: narrow exceptions where possible; keep this as last-resort
            if self.metrics_collector:
                self.metrics_collector.record_failed_batch(force, year_month, str(e))
            raise  # Re-raise for caller to handle