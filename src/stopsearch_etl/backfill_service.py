from dataclasses import dataclass
from typing import List

from .api import PoliceApiClient, ApiError
from .etl_service import EtlService


@dataclass
class BackfillResult:
    """Result of a backfill operation"""

    # summary of a backfill run
    force: str
    total_records: int
    months_processed: int
    months_failed: int = 0


class BackfillService:
    """Pulls all historical months for a force and runs ETL per month"""

    def __init__(self, api_client: PoliceApiClient, etl_service: EtlService):
        self.api_client = api_client
        self.etl_service = etl_service

    def backfill_force(self, force: str) -> BackfillResult:
        """
        Backfill all available historical data for a police force

        Returns:
            BackfillResult with summary of the operation
        """
        result = BackfillResult(
            force=force,
            total_records=0,
            months_processed=0,
            months_failed=0
        )

        try:
            # Discover available months for this force
            # TODO: month order depends on API; sort if you want deterministic runs (e.g. newest→oldest)
            available_months = self.api_client.get_available_months(force)

            if not available_months:
                return result

            # run etl by each month
            for month in available_months:
                try:
                    records_saved = self.etl_service.extract_transform_load(force, month)
                    result.total_records += records_saved
                    result.months_processed += 1

                    # quick progress ping; TODO: swap for proper logging
                    print(f"Processed {force} {month}: {records_saved} records")

                except Exception as e:
                    result.months_failed += 1
                    print(f"Failed to process {force} {month}: {e}")
                    # continue

        except ApiError as e:
            # couldn't even get the months list
            print(f"Failed to get available months for {force}: {e}")
            # TODO: decide if you want to re-raise here for callers to handle vs. return an empty summary


        return result